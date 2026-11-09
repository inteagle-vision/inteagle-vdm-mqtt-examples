package com.inteagle.vdm.mqtt.sdk;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 系统集成商平台使用的 VDM MQTT SDK 客户端。
 *
 * <p>每个实例只绑定一个设备 Topic 和一张 RPC pending 表。不同设备或不同云连接必须创建 独立逻辑实例；多设备统一使用 VdmMqttGateway
 * 复用连接。相同设备的多个客户端仍共享响应 Topic，通过各自的请求 ID 表匹配响应。
 *
 * <p>纯 SDK 的 handler 在传输实现的接收线程执行，不能在里面调用同步 call()；应转交自己的 工作线程。Spring Boot Starter
 * 已封装线程隔离，业务监听器可以直接调用 RPC。 默认 Paho 适配器使用 clean session 和内存持久化，不提供跨进程重启的可靠队列。
 */
public final class VdmMqttClient implements AutoCloseable {
  /**
   * 单设备连接配置。host 不含协议前缀；connectTimeout 是连接及订阅的总等待预算。 subscriptionSuffixes 是 Topic 后缀列表，例如仅位移使用
   * telemetry，仅图片使用 image； RPC 调用必须订阅 rpc/resp。旧构造器保留 # 全订阅行为，新接入应显式选择。 clientId 留空时生成随机
   * ID；显式指定时须保证同一 Broker 上各实例唯一。
   */
  public record Config(
      String host,
      int port,
      VdmTopics topics,
      PayloadFormat payloadFormat,
      String username,
      String password,
      String clientId,
      int qos,
      Duration connectTimeout,
      List<String> subscriptionSuffixes) {
    /** 兼容旧示例的全设备订阅；新代码优先使用带 subscriptionSuffixes 的构造器。 */
    public Config(
        String host,
        int port,
        VdmTopics topics,
        PayloadFormat payloadFormat,
        String username,
        String password,
        String clientId,
        int qos,
        Duration connectTimeout) {
      this(
          host,
          port,
          topics,
          payloadFormat,
          username,
          password,
          clientId,
          qos,
          connectTimeout,
          null);
    }

    public Config {
      if (host == null || host.isBlank() || port < 1 || port > 65535) {
        throw new IllegalArgumentException("MQTT host/port 无效");
      }
      Objects.requireNonNull(topics, "topics");
      Objects.requireNonNull(payloadFormat, "payloadFormat");
      subscriptionSuffixes =
          subscriptionSuffixes == null ? List.of("#") : List.copyOf(subscriptionSuffixes);
      if (subscriptionSuffixes.isEmpty()
          || subscriptionSuffixes.stream()
              .anyMatch(
                  suffix ->
                      !List.of(
                              "#",
                              "telemetry",
                              "attributes",
                              "event",
                              "3A",
                              "image",
                              "rpc/req",
                              "rpc/resp")
                          .contains(suffix))) {
        throw new IllegalArgumentException("subscriptionSuffixes contains an unsupported Topic");
      }
      if (qos < 0 || qos > 2) {
        throw new IllegalArgumentException("MQTT qos 必须是 0、1 或 2");
      }
      if (connectTimeout == null || connectTimeout.isZero() || connectTimeout.isNegative()) {
        connectTimeout = Duration.ofSeconds(10);
      }
      if (clientId == null || clientId.isBlank()) {
        clientId = "vdm-sdk-java-" + UUID.randomUUID().toString().substring(0, 12);
      }
    }
  }

  private record Pending(String expectedField, CompletableFuture<DecodedPayload> future) {}

  private final Config config;
  private final VdmCodec codec;
  private final java.util.function.Predicate<String> subscriptionAvailable;
  private final Consumer<DecodedPayload> handler;
  private final Consumer<Throwable> errorHandler;
  private final VdmMqttTransport transport;
  private final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();
  // 相同设备共享 RPC 响应 Topic，随机化起始 ID 以降低多个独立客户端撞号的概率。
  private final AtomicInteger reqIds =
      new AtomicInteger(new SecureRandom().nextInt(1, Integer.MAX_VALUE));
  private volatile boolean closing;

  /**
   * 创建客户端但不连接网络，调用 start() 后才连接并订阅。
   *
   * @param transport MQTT 传输实现；关闭本客户端时会调用其 close()
   * @param handler 解码后的消息回调，可为 null；纯 SDK 回调中不要阻塞等待 RPC
   * @param errorHandler 连接、订阅、解码或消息回调失败的处理器，可为 null
   */
  public VdmMqttClient(
      Config config,
      VdmMqttTransport transport,
      Consumer<DecodedPayload> handler,
      Consumer<Throwable> errorHandler) {
    this(
        config,
        transport,
        handler,
        errorHandler,
        suffix ->
            config.subscriptionSuffixes().contains("#")
                || config.subscriptionSuffixes().contains(suffix));
  }

  /** Gateway 使用当前订阅快照校验 RPC，避免运行时订阅变更后仍读取旧 Config。 */
  VdmMqttClient(
      Config config,
      VdmMqttTransport transport,
      Consumer<DecodedPayload> handler,
      Consumer<Throwable> errorHandler,
      java.util.function.Predicate<String> subscriptionAvailable) {
    this.subscriptionAvailable = subscriptionAvailable;
    this.config = Objects.requireNonNull(config, "config");
    this.transport = Objects.requireNonNull(transport, "transport");
    this.codec = new VdmCodec(config.payloadFormat());
    this.handler = handler;
    this.errorHandler = errorHandler;
  }

  /** 获取当前格式的编解码器，可用于离线验证 RPC 参数。 */
  public VdmCodec codec() {
    return codec;
  }

  /** 标准 vdm/{deviceId} Topic 中的设备 ID；业务标靶键必须包含该 ID。 */
  public String deviceId() {
    String base = config.topics().baseTopic();
    return base.substring(base.lastIndexOf('/') + 1);
  }

  /** 获取统一业务标靶模型，JSON/Protobuf 差异由 SDK 处理。需先订阅 rpc/resp。 */
  public List<VdmTarget> getTargets(Duration timeout) throws Exception {
    return VdmTargets.fromResponse(deviceId(), call("getTargets", Map.of(), timeout));
  }

  /** 使用 10 秒响应超时查询标靶列表。 */
  public List<VdmTarget> getTargets() throws Exception {
    return getTargets(Duration.ofSeconds(10));
  }

  /**
   * 请求指定传感器拍摄普通 JPEG。调用前先订阅 image 和 rpc/resp，避免图片先到而丢失。 返回值只表示 RPC 成功，接收结果通过 ImageFrame
   * 监听器交付；不自动进行失败重试。
   */
  public SnapshotReceipt requestSnapshot(int sensorId, Duration timeout) throws Exception {
    if (sensorId < 0 || sensorId > 255) throw new IllegalArgumentException("sensorId 必须在 0..255 内");
    if (!subscriptionAvailable.test("image")) throw new IllegalStateException("拍照前必须订阅该设备的 image");
    var response = call("snapshot", Map.of("sensorId", sensorId), timeout);
    return new SnapshotReceipt(deviceId(), sensorId, codec.responseInfo(response.value()).reqId());
  }

  /** 默认单目传感器 0，使用 10 秒响应超时。 */
  public SnapshotReceipt requestSnapshot() throws Exception {
    return requestSnapshot(0, Duration.ofSeconds(10));
  }

  /**
   * 等待连接及所有订阅的 SUBACK；返回后才表示可以接收数据。 后续断线由传输实现自动重连并重新订阅；失败时调用方仍应 close() 释放资源。
   *
   * @throws Exception Broker 不可达、订阅被拒绝、超时或线程被中断
   */
  public void start() throws Exception {
    if (closing) throw new IllegalStateException("VDM MQTT client 已关闭，不能重新启动");
    transport.start(
        config.subscriptionSuffixes().stream().map(config.topics()::topic).toList(),
        config.qos(),
        config.connectTimeout(),
        new VdmMqttTransport.Listener() {
          @Override
          public void onMessage(String topic, byte[] payload) {
            receive(topic, payload);
          }

          @Override
          public void onDisconnected(Throwable cause) {
            connectionLost(cause);
          }

          @Override
          public void onError(Throwable error) {
            report(error);
          }
        });
  }

  /** 发布已经编码的 Payload 并等待 MQTT 投递完成；这不是设备业务处理成功的确认。 */
  public void publishRaw(String topic, byte[] payload, boolean retained) throws Exception {
    if (closing) throw new IllegalStateException("VDM MQTT client 已关闭");
    transport.publish(topic, payload, config.qos(), retained, Duration.ofSeconds(5));
  }

  /** 按 UTF-8 发布文本，适用于 JSON 或联调控制消息。 */
  public void publishRaw(String topic, String payload, boolean retained) throws Exception {
    publishRaw(topic, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), retained);
  }

  /**
   * 编码公开 RPC、发布请求，并等待匹配的响应；无论成功失败都会移除 pending 记录。
   *
   * @param method 公开方法名，例如 getAttr，不接受设备内部命令
   * @param params 当前 Payload 格式对应的参数，空参数可传 Map.of()
   * @param reqId 0 表示自动分配；非零值必须在当前客户端的在途请求中唯一
   * @param timeout 发布完成后等待设备响应的时间
   * @param allowError true 时把设备非零 code 原样返回；false 时抛出 RpcException
   * @throws Exception 编码、发布、等待或响应类型校验失败；超时不代表设备未执行命令
   */
  public DecodedPayload call(
      String method, Map<String, ?> params, int reqId, Duration timeout, boolean allowError)
      throws Exception {
    if (!subscriptionAvailable.test("rpc/resp")) {
      throw new IllegalStateException("RPC requires the rpc/resp subscription");
    }
    if (timeout == null || timeout.toMillis() < 1)
      throw new IllegalArgumentException("RPC timeout 必须至少为 1ms");
    int id = reqId == 0 ? nextReqId() : reqId;
    VdmCodec.RpcEncoding encoding = codec.encodeRpcRequest(method, params, id);
    Pending call = new Pending(encoding.expectedResponseField(), new CompletableFuture<>());
    if (pending.putIfAbsent(id, call) != null) {
      throw new IllegalArgumentException("reqId 已在当前连接等待响应: " + id);
    }
    try {
      publishRaw(config.topics().rpcRequest(), encoding.payload(), false);
      DecodedPayload result = call.future().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      VdmCodec.ResponseInfo response = codec.responseInfo(result.value());
      if (response.reqId() != id) {
        throw new IllegalStateException(
            "RPC reqId 不匹配: request=" + id + " response=" + response.reqId());
      }
      if (response.code() != 0 && !allowError) {
        throw new RpcException(id, response.code(), response.message());
      }
      if (response.code() == 0
          && config.payloadFormat() == PayloadFormat.PROTOBUF
          && !Objects.equals(call.expectedField(), response.responseField())) {
        throw new IllegalStateException(
            "RPC response oneof 不匹配: expected="
                + call.expectedField()
                + " actual="
                + response.responseField());
      }
      return result;
    } finally {
      pending.remove(id, call);
    }
  }

  /** 自动生成请求 ID，并把设备非零 code 视为失败的常用 RPC 入口。 */
  public DecodedPayload call(String method, Map<String, ?> params, Duration timeout)
      throws Exception {
    return call(method, params, 0, timeout, false);
  }

  /** 按告警事件查询抓拍证据状态；eventId 使用 uint64 的 Java long 位模式。 */
  public DecodedPayload getEvidenceStatus(long eventId, Duration timeout) throws Exception {
    requireEventId(eventId);
    return call(
        "getEvidenceStatus",
        Map.of("eventId", Long.toUnsignedString(eventId), "kind", snapshotKind()),
        timeout);
  }

  /** 请求补传已有抓拍证据；受理成功后仍须接收完整包并 ACK。 */
  public DecodedPayload retryEvidence(long eventId, Duration timeout) throws Exception {
    requireEventId(eventId);
    return call(
        "retryEvidence",
        Map.of("eventId", Long.toUnsignedString(eventId), "kind", snapshotKind()),
        timeout);
  }

  /**
   * 完整落盘并通过 SHA-256/USTAR 校验后，使用接收证据的同一 MQTT 连接确认。 packageSha256 必须为 64 位小写十六进制；普通 JPEG 不走此确认流程。
   */
  public DecodedPayload ackEvidencePackage(long eventId, String packageSha256, Duration timeout)
      throws Exception {
    requireEventId(eventId);
    if (packageSha256 == null || !packageSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("packageSha256 必须是 64 个小写十六进制字符");
    }
    return call(
        "ackEvidencePackage",
        Map.of(
            "eventId", Long.toUnsignedString(eventId),
            "kind", snapshotKind(),
            "packageSha256", packageSha256),
        timeout);
  }

  private String snapshotKind() {
    return config.payloadFormat() == PayloadFormat.PROTOBUF ? "EVIDENCE_KIND_SNAPSHOT" : "SNAPSHOT";
  }

  private static void requireEventId(long eventId) {
    if (eventId == 0) {
      throw new IllegalArgumentException("eventId 必须是非零整数");
    }
  }

  private int nextReqId() {
    int value = reqIds.incrementAndGet();
    if (value == 0) {
      value = reqIds.incrementAndGet();
    }
    return value;
  }

  /** Gateway 取消响应订阅时立即结束该设备的在途 RPC，其他设备不受影响。 */
  void failPendingRequests(Throwable cause) {
    pending.forEach((id, call) -> call.future().completeExceptionally(cause));
  }

  /** 断线时立即结束在途 RPC，避免业务一直等到各自超时。 */
  private void connectionLost(Throwable cause) {
    if (!closing) {
      pending.forEach(
          (id, call) ->
              call.future().completeExceptionally(new IllegalStateException("MQTT 连接断开", cause)));
      pending.clear();
      report(cause);
    }
  }

  /** 先完成 RPC 响应关联，再通知业务；业务回调不能阻塞 MQTT 接收线程。 */
  private void receive(String topic, byte[] payload) {
    try {
      DecodedPayload decoded = codec.decode(topic, config.topics(), payload);
      if (decoded.suffix().equals("rpc/resp")) {
        VdmCodec.ResponseInfo info = codec.responseInfo(decoded.value());
        Pending call = pending.get(info.reqId());
        if (call != null) {
          call.future().complete(decoded);
        }
      }
      if (handler != null) {
        handler.accept(decoded);
      }
    } catch (Throwable exception) {
      report(exception);
    }
  }

  private void report(Throwable exception) {
    if (exception != null && errorHandler != null) {
      errorHandler.accept(exception);
    }
  }

  /** 幂等释放连接及 pending 请求；关闭后的实例不能再次 start()。 */
  @Override
  public synchronized void close() throws Exception {
    if (closing) return;
    closing = true;
    pending.forEach(
        (id, call) ->
            call.future()
                .completeExceptionally(new IllegalStateException("VDM MQTT client stopped")));
    pending.clear();
    transport.close();
  }
}
