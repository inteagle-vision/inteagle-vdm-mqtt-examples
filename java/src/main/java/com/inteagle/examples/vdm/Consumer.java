package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.DecodedPayload;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 使用 Java SDK 订阅、强类型解析并输出 VDM MQTT Payload。 */
public final class Consumer {
  private static final Set<String> REQUIRED_SUFFIXES = Set.of(
      "telemetry", "attributes", "event", "3A", "rpc/req", "rpc/resp", "image");

  private final PayloadFormat format = PayloadFormat.parse(setting("VDM_PAYLOAD_FORMAT", "protobuf"));
  private final String runId = setting("VDM_RUN_ID", "manual");
  private final String name = setting("VDM_CONSUMER_NAME", "java");
  private final Duration timeout = Duration.ofMillis(
      Math.round(Double.parseDouble(setting("VDM_WAIT_TIMEOUT", "45")) * 1000));
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Set<String> seen = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean resultSent = new AtomicBoolean(false);
  private final AtomicBoolean rpcStarted = new AtomicBoolean(false);
  private final AtomicBoolean rpcPassed = new AtomicBoolean(false);
  private VdmMqttClient client;

  private static String setting(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }

  private String controlTopic(String kind) {
    return "vdm-example/" + runId + "/" + kind + "/" + name;
  }

  private void publishControl(String kind, String value) throws Exception {
    client.publishRaw(controlTopic(kind), value.getBytes(StandardCharsets.UTF_8), true);
  }

  private void fail(Throwable error) {
    String reason = error == null ? "unknown" : String.valueOf(error.getMessage());
    if (!resultSent.compareAndSet(false, true)) {
      return;
    }
    System.out.printf("FAIL consumer=%s reason=%s%n", name, reason);
    try {
      publishControl("result", "FAIL:" + reason);
    } catch (Exception exception) {
      System.out.printf("FAIL consumer=%s 无法上报结果: %s%n", name, exception.getMessage());
    }
  }

  private void onMessage(DecodedPayload message) {
    try {
      if (!REQUIRED_SUFFIXES.contains(message.suffix())) {
        throw new IllegalArgumentException("未支持的 Topic: " + message.suffix());
      }
      System.out.printf(
          "DECODED consumer=%s profile=%s topic=%s bytes=%d data=%s%n",
          name,
          format,
          message.suffix(),
          message.raw().length,
          objectMapper.writeValueAsString(message.data()));
      seen.add(message.suffix());
      if (message.suffix().equals("telemetry") && rpcStarted.compareAndSet(false, true)) {
        Thread thread = new Thread(this::testRpc, "vdm-java-rpc-test");
        thread.setDaemon(true);
        thread.start();
      }
      completeIfReady();
    } catch (Exception exception) {
      fail(exception);
    }
  }

  private void testRpc() {
    try {
      DecodedPayload response = client.call(
          "getAttr",
          Map.of("keys", List.of("deviceId", "fwVer")),
          13_001,
          Duration.ofSeconds(10),
          false);
      String deviceId = format == PayloadFormat.PROTOBUF
          ? response.data().path("getAttr").path("attributes").path("deviceId").asText()
          : response.data().path("data").path("deviceId").asText();
      if (!deviceId.equals("DEMO001")) {
        throw new AssertionError("RPC getAttr.deviceId 不匹配: " + deviceId);
      }
      rpcPassed.set(true);
      System.out.printf(
          "RPC_PASS consumer=%s method=getAttr req_id=13001 data=%s%n",
          name, objectMapper.writeValueAsString(response.data()));
      completeIfReady();
    } catch (Throwable exception) {
      fail(exception);
    }
  }

  private void completeIfReady() throws Exception {
    if (rpcPassed.get()
        && seen.containsAll(REQUIRED_SUFFIXES)
        && resultSent.compareAndSet(false, true)) {
      publishControl("result", "PASS");
      ArrayList<String> topics = new ArrayList<>(seen);
      topics.sort(String::compareTo);
      System.out.printf(
          "PASS consumer=%s profile=%s topics=%s rpc=getAttr%n",
          name, format, String.join(",", topics));
    }
  }

  private void run() throws Exception {
    int port = Integer.parseInt(setting("MQTT_PORT", "1883"));
    VdmTopics topics = new VdmTopics(setting("VDM_BASE_TOPIC", "vdm/DEMO001"));
    client = new VdmMqttClient(
        new VdmMqttClient.Config(
            setting("MQTT_HOST", "127.0.0.1"),
            port,
            topics,
            format,
            null,
            null,
            "vdm-example-" + name + "-" + runId,
            1,
            timeout),
        this::onMessage,
        this::fail);
    client.start();
    publishControl("ready", "READY");
    System.out.printf("READY consumer=%s profile=%s%n", name, format);
    new CountDownLatch(1).await(Long.MAX_VALUE, TimeUnit.DAYS);
  }

  public static void main(String[] args) {
    try {
      new Consumer().run();
    } catch (Exception exception) {
      System.err.println("FAIL consumer=java reason=" + exception.getMessage());
      System.exit(1);
    }
  }
}
