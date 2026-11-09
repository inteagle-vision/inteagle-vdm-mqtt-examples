package com.inteagle.vdm.mqtt.sdk;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 面向系统集成商的多设备接入入口：复用一条连接，按实际 Topic 路由设备和 RPC 响应。
 *
 * <p>支持 MQTT 标准 +/# 通配符及运行时增减订阅。通配范围内的新设备自动接入。 device(id) 是逻辑客户端，不创建 TCP 连接；其连接生命周期由 Gateway 统一管理。
 */
public final class VdmMqttGateway implements AutoCloseable {
  private static final List<String> SUFFIXES =
      List.of("telemetry", "attributes", "image", "event", "3A", "rpc/req", "rpc/resp");
  private final VdmMqttClient.Config config;
  private final VdmMqttTransport transport;
  private final BiConsumer<String, DecodedPayload> handler;
  private final Consumer<Throwable> errors;
  private final ConcurrentHashMap<String, Route> routes = new ConcurrentHashMap<>();
  private volatile List<String> topicFilters;
  private volatile boolean started;
  private volatile boolean closed;

  /** 简单接法：所选设备统一订阅 Config 中的后缀；设备列表与 allDevices 二选一。 */
  public VdmMqttGateway(
      VdmMqttClient.Config config,
      List<String> deviceIds,
      boolean allDevices,
      VdmMqttTransport transport,
      BiConsumer<String, DecodedPayload> handler,
      Consumer<Throwable> errors) {
    this(config, filtersFor(config, deviceIds, allDevices), transport, handler, errors);
  }

  /** 自由订阅接法：直接传 MQTT 标准 Topic Filter；空列表表示先连接、后由业务动态订阅。 */
  public VdmMqttGateway(
      VdmMqttClient.Config config,
      List<String> topicFilters,
      VdmMqttTransport transport,
      BiConsumer<String, DecodedPayload> handler,
      Consumer<Throwable> errors) {
    this.config = java.util.Objects.requireNonNull(config);
    this.transport = java.util.Objects.requireNonNull(transport);
    this.topicFilters = topicFilters.stream().map(MqttTopicFilters::validate).distinct().toList();
    this.handler = handler;
    this.errors = errors;
  }

  private static List<String> filtersFor(
      VdmMqttClient.Config config, List<String> devices, boolean all) {
    if (all == !devices.isEmpty())
      throw new IllegalArgumentException("选择设备列表或 all-devices，不能同时选择或都不选择");
    return (all
            ? java.util.stream.Stream.of("+")
            : devices.stream()
                .map(
                    id -> {
                      String base = VdmTopics.forDevice(id).baseTopic();
                      return base.substring(4);
                    })
                .distinct()
                .sorted())
        .flatMap(
            id -> config.subscriptionSuffixes().stream().map(suffix -> "vdm/" + id + "/" + suffix))
        .toList();
  }

  /** 当前由本 Gateway 管理的订阅过滤器快照；运行时变更不写回 application.yml。 */
  public List<String> topicFilters() {
    return topicFilters;
  }

  private boolean subscribed(String deviceId, String suffix) {
    String topic = VdmTopics.forDevice(deviceId).topic(suffix);
    return topicFilters.stream().anyMatch(filter -> MqttTopicFilters.matches(filter, topic));
  }

  /** 获取允许订阅范围内的设备逻辑客户端；调用 RPC 时还会单独检查该设备的 rpc/resp。 */
  public VdmMqttClient device(String deviceId) {
    String canonical = VdmTopics.forDevice(deviceId).baseTopic().substring(4);
    if (closed) throw new IllegalStateException("VDM gateway 已关闭");
    if (SUFFIXES.stream().noneMatch(suffix -> subscribed(canonical, suffix))) {
      throw new IllegalArgumentException("设备不在当前订阅范围: " + canonical);
    }
    return routes.computeIfAbsent(canonical, this::newRoute).client;
  }

  private Route newRoute(String deviceId) {
    var route = new Route();
    var deviceConfig =
        new VdmMqttClient.Config(
            config.host(),
            config.port(),
            VdmTopics.forDevice(deviceId),
            config.payloadFormat(),
            config.username(),
            config.password(),
            config.clientId(),
            config.qos(),
            config.connectTimeout(),
            List.of("#"));
    route.client =
        new VdmMqttClient(
            deviceConfig,
            route,
            message -> {
              if (handler != null) handler.accept(deviceId, message);
            },
            errors,
            suffix -> subscribed(deviceId, suffix));
    try {
      route.client.start();
    } catch (Exception error) {
      throw new IllegalStateException(error);
    }
    return route;
  }

  /** 建立一次物理连接；启动成功表示初始订阅已被 Broker 接受。 */
  public synchronized void start() throws Exception {
    if (closed) throw new IllegalStateException("VDM gateway 已关闭");
    if (started) return;
    started = true;
    try {
      transport.start(
          topicFilters,
          config.qos(),
          config.connectTimeout(),
          new VdmMqttTransport.Listener() {
            @Override
            public void onMessage(String topic, byte[] payload) {
              if (closed
                  || topicFilters.stream()
                      .noneMatch(filter -> MqttTopicFilters.matches(filter, topic))) return;
              String[] parts = topic.split("/", 3);
              // 通配符可能覆盖其他业务 Topic；VDM SDK 仅解码公开的 vdm/{deviceId}/{suffix}。
              if (parts.length != 3
                  || !parts[0].equals("vdm")
                  || parts[1].isBlank()
                  || !SUFFIXES.contains(parts[2])) return;
              try {
                device(parts[1]);
                Route route = routes.get(parts[1]);
                if (route != null) route.listener.onMessage(topic, payload);
              } catch (RuntimeException error) {
                onError(error);
              }
            }

            @Override
            public void onDisconnected(Throwable error) {
              routes.values().forEach(route -> route.listener.onDisconnected(error));
            }

            @Override
            public void onError(Throwable error) {
              if (errors != null) errors.accept(error);
            }
          });
    } catch (Exception error) {
      started = false;
      throw error;
    }
  }

  /**
   * 在当前连接增加一个标准 MQTT Filter，等待 SUBACK；重复添加相同 Filter 为幂等操作。 可订阅任意已知消息类型，不局限于 telemetry。调用方应从业务线程调用。
   */
  public synchronized void subscribe(String topicFilter) throws Exception {
    requireRunning();
    MqttTopicFilters.validate(topicFilter);
    if (topicFilters.contains(topicFilter)) return;
    List<String> previous = topicFilters;
    topicFilters =
        java.util.stream.Stream.concat(previous.stream(), java.util.stream.Stream.of(topicFilter))
            .toList();
    try {
      // 先开放本地路由，避免 SUBACK 前后立即到达的 retained 消息被丢弃。
      transport.subscribe(topicFilter, config.qos(), config.connectTimeout());
    } catch (Exception error) {
      topicFilters = previous;
      invalidateUnavailableRoutes();
      throw error;
    }
  }

  /**
   * 取消一个原样匹配的订阅 Filter，等待 UNSUBACK。取消具体设备不会抵消仍存在的通配订阅。 例如仍订阅 vdm/+/image 时，取消 vdm/A/image 后仍能收到 A
   * 的图片，这是 MQTT 标准语义。
   */
  public synchronized void unsubscribe(String topicFilter) throws Exception {
    requireRunning();
    MqttTopicFilters.validate(topicFilter);
    if (!topicFilters.contains(topicFilter)) return;
    transport.unsubscribe(topicFilter, config.connectTimeout());
    topicFilters = topicFilters.stream().filter(filter -> !filter.equals(topicFilter)).toList();
    invalidateUnavailableRoutes();
  }

  private void invalidateUnavailableRoutes() {
    routes.forEach(
        (id, route) -> {
          if (!subscribed(id, "rpc/resp")) {
            route.client.failPendingRequests(new IllegalStateException("该设备的 RPC 响应订阅已取消"));
          }
        });
  }

  private void requireRunning() {
    if (!started || closed) throw new IllegalStateException("VDM gateway 尚未启动或已关闭");
  }

  /** 是否已经启动且尚未关闭；断线重连期间仍为 true，不代表网络健康检查。 */
  public boolean isRunning() {
    return started && !closed;
  }

  @Override
  public synchronized void close() throws Exception {
    if (closed) return;
    closed = true;
    started = false;
    try {
      for (Route route : routes.values()) route.client.close();
      routes.clear();
    } finally {
      transport.close();
    }
  }

  /** 每个设备只维护响应路由；不拥有共享物理连接。 */
  private final class Route implements VdmMqttTransport {
    private VdmMqttClient client;
    private Listener listener;

    @Override
    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {
      this.listener = listener;
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception {
      requireRunning();
      transport.publish(topic, payload, qos, retained, timeout);
    }

    @Override
    public void close() {
      /* 连接由 Gateway 统一管理。 */
    }
  }
}
