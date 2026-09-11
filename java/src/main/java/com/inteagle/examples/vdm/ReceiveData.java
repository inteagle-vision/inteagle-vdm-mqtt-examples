package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/** Receive telemetry and attributes; optionally query attributes once. */
public final class ReceiveData {
  private ReceiveData() {}

  public static void main(String[] args) {
    try {
      run(args);
    } catch (Exception error) {
      System.err.println("ERROR " + error.getMessage());
      System.exit(1);
    }
  }

  private static void run(String[] args) throws Exception {
    boolean queryAttributes = false;
    for (String argument : args) {
      switch (argument) {
        case "--query-attributes" -> queryAttributes = true;
        case "--help", "-h" -> {
          System.out.println("ReceiveData [--query-attributes]");
          System.out.println("Set MQTT_HOST and VDM_DEVICE_ID; optional MQTT_PORT, "
              + "MQTT_USERNAME, MQTT_PASSWORD, VDM_PAYLOAD_FORMAT (protobuf or json).");
          return;
        }
        default -> throw new IllegalArgumentException("Unknown argument: " + argument);
      }
    }

    List<String> subscriptions = queryAttributes
        ? List.of("telemetry", "attributes", "rpc/resp")
        : List.of("telemetry", "attributes");
    var config = new VdmMqttClient.Config(
        requiredEnv("MQTT_HOST"),
        Integer.parseInt(System.getenv().getOrDefault("MQTT_PORT", "1883")),
        VdmTopics.forDevice(requiredEnv("VDM_DEVICE_ID")),
        PayloadFormat.parse(System.getenv().getOrDefault("VDM_PAYLOAD_FORMAT", "protobuf")),
        System.getenv("MQTT_USERNAME"), System.getenv("MQTT_PASSWORD"), null, 1,
        Duration.ofSeconds(10), subscriptions);
    var client = new VdmMqttClient(config, message -> {
      if (message.suffix().equals("telemetry") || message.suffix().equals("attributes")) {
        System.out.println(message.suffix() + " " + message.data());
      }
    }, error -> System.err.println("DECODE_ERROR " + error.getMessage()));

    var stop = new CountDownLatch(1);
    var closed = new AtomicBoolean();
    Runnable close = () -> {
      if (closed.compareAndSet(false, true)) {
        try {
          client.close();
        } catch (Exception error) {
          System.err.println("CLOSE_ERROR " + error.getMessage());
        }
      }
      stop.countDown();
    };
    var shutdown = new Thread(close, "vdm-receive-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdown);
    try {
      client.start();
      System.out.println("READY waiting for telemetry/attributes; Ctrl-C to stop");
      if (queryAttributes) {
        // Blocking RPC runs on this main thread, never on Paho's callback thread.
        var response = client.call("getAttr",
            Map.of("keys", List.of("deviceId", "deviceModel", "fwVer", "measureStatus")),
            ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE),
            Duration.ofSeconds(30), false);
        ObjectNode fields = ((ObjectNode) response.data()).deepCopy();
        fields.put("code", 0);
        System.out.println("RESPONSE " + fields);
      }
      stop.await();
    } finally {
      close.run();
      try {
        Runtime.getRuntime().removeShutdownHook(shutdown);
      } catch (IllegalStateException ignored) {
        // JVM shutdown is already running the hook.
      }
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Set " + name);
    }
    return value;
  }
}
