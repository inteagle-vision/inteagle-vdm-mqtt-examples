package com.inteagle.vdm.mqtt.sdk;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * 客户云侧 VDM MQTT SDK 客户端。
 *
 * <p>每个实例只绑定一个设备 Topic 和一张 RPC pending 表。不同设备或不同云连接必须创建
 * 独立实例，因此一个连接上的 RPC 响应不会被另一个连接消费。
 */
public final class VdmMqttClient implements AutoCloseable, MqttCallbackExtended {
  public record Config(
      String host,
      int port,
      VdmTopics topics,
      PayloadFormat payloadFormat,
      String username,
      String password,
      String clientId,
      int qos,
      Duration connectTimeout) {
    public Config {
      if (host == null || host.isBlank() || port < 1 || port > 65535) {
        throw new IllegalArgumentException("MQTT host/port 无效");
      }
      Objects.requireNonNull(topics, "topics");
      Objects.requireNonNull(payloadFormat, "payloadFormat");
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
  private final Consumer<DecodedPayload> handler;
  private final Consumer<Throwable> errorHandler;
  private final MqttAsyncClient client;
  private final MqttConnectOptions connectOptions;
  private final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();
  private final AtomicInteger reqIds = new AtomicInteger();
  private volatile CountDownLatch subscribed = new CountDownLatch(1);
  private volatile boolean closing;

  public VdmMqttClient(
      Config config,
      Consumer<DecodedPayload> handler,
      Consumer<Throwable> errorHandler) throws MqttException {
    this.config = config;
    this.codec = new VdmCodec(config.payloadFormat());
    this.handler = handler;
    this.errorHandler = errorHandler;
    this.client = new MqttAsyncClient(
        "tcp://" + config.host() + ":" + config.port(), config.clientId());
    this.client.setCallback(this);
    this.connectOptions = new MqttConnectOptions();
    connectOptions.setCleanSession(true);
    connectOptions.setAutomaticReconnect(true);
    connectOptions.setConnectionTimeout(2);
    if (config.username() != null && !config.username().isBlank()) {
      connectOptions.setUserName(config.username());
      connectOptions.setPassword(config.password() == null ? new char[0] : config.password().toCharArray());
    }
  }

  public VdmCodec codec() {
    return codec;
  }

  public void start() throws Exception {
    long deadline = System.nanoTime() + config.connectTimeout().toNanos();
    Throwable lastError = null;
    while (System.nanoTime() < deadline && !client.isConnected()) {
      try {
        IMqttToken token = client.connect(connectOptions);
        token.waitForCompletion(3_000);
      } catch (MqttException exception) {
        lastError = exception;
        Thread.sleep(200);
      }
    }
    if (!client.isConnected()) {
      throw new IllegalStateException("连接 MQTT Broker 超时", lastError);
    }
    long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
    if (!subscribed.await(remainingMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("订阅 VDM Topic 超时: " + config.topics().wildcard());
    }
  }

  public void publishRaw(String topic, byte[] payload, boolean retained) throws MqttException {
    MqttMessage message = new MqttMessage(payload);
    message.setQos(config.qos());
    message.setRetained(retained);
    IMqttDeliveryToken token = client.publish(topic, message);
    token.waitForCompletion(5_000);
  }

  public void publishRaw(String topic, String payload, boolean retained) throws MqttException {
    publishRaw(topic, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), retained);
  }

  public DecodedPayload call(
      String method,
      Map<String, ?> params,
      int reqId,
      Duration timeout,
      boolean allowError) throws Exception {
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
            "RPC response oneof 不匹配: expected=" + call.expectedField()
                + " actual=" + response.responseField());
      }
      return result;
    } finally {
      pending.remove(id, call);
    }
  }

  public DecodedPayload call(String method, Map<String, ?> params, Duration timeout)
      throws Exception {
    return call(method, params, 0, timeout, false);
  }

  public DecodedPayload getEvidenceStatus(long eventId, Duration timeout) throws Exception {
    requireEventId(eventId);
    return call(
        "getEvidenceStatus",
        Map.of("eventId", Long.toUnsignedString(eventId), "kind", snapshotKind()),
        timeout);
  }

  public DecodedPayload retryEvidence(long eventId, Duration timeout) throws Exception {
    requireEventId(eventId);
    return call(
        "retryEvidence",
        Map.of("eventId", Long.toUnsignedString(eventId), "kind", snapshotKind()),
        timeout);
  }

  public DecodedPayload ackEvidencePackage(
      long eventId, String packageSha256, Duration timeout) throws Exception {
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
    return config.payloadFormat() == PayloadFormat.PROTOBUF
        ? "EVIDENCE_KIND_SNAPSHOT"
        : "SNAPSHOT";
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

  @Override
  public void connectComplete(boolean reconnect, String serverURI) {
    if (reconnect) {
      subscribed = new CountDownLatch(1);
    }
    try {
      client.subscribe(config.topics().wildcard(), config.qos(), null, new IMqttActionListener() {
        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
          subscribed.countDown();
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
          report(new IllegalStateException("订阅 VDM Topic 失败", exception));
        }
      });
    } catch (MqttException exception) {
      report(exception);
    }
  }

  @Override
  public void connectionLost(Throwable cause) {
    if (!closing) {
      pending.forEach((id, call) -> call.future().completeExceptionally(
          new IllegalStateException("MQTT 连接断开", cause)));
      pending.clear();
      report(cause);
    }
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      DecodedPayload decoded = codec.decode(topic, config.topics(), message.getPayload());
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

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // Paho 已处理 QoS ACK；SDK 调用方不需要维护额外状态。
  }

  private void report(Throwable exception) {
    if (exception != null && errorHandler != null) {
      errorHandler.accept(exception);
    }
  }

  @Override
  public void close() throws MqttException {
    closing = true;
    pending.forEach((id, call) -> call.future().completeExceptionally(
        new IllegalStateException("VDM MQTT client stopped")));
    pending.clear();
    if (client.isConnected()) {
      client.disconnect(1_000).waitForCompletion(2_000);
    }
    client.close();
  }
}
