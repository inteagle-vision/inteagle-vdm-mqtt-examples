package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Durable, idempotent consumer for VDM alarm transition messages. */
public final class AlarmNotificationConsumer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AlarmNotificationConsumer() {}

  public static void main(String[] args) throws Exception {
    Path database = parseDatabase(args);
    String deviceId = requiredSetting("VDM_DEVICE_ID");
    PayloadFormat format = PayloadFormat.parse(setting("VDM_PAYLOAD_FORMAT", "protobuf"));
    VdmMqttClient.Config config = new VdmMqttClient.Config(
        requiredSetting("MQTT_HOST"),
        Integer.parseInt(setting("MQTT_PORT", "1883")),
        VdmTopics.forDevice(deviceId),
        format,
        System.getenv("MQTT_USERNAME"),
        System.getenv("MQTT_PASSWORD"),
        setting("MQTT_CLIENT_ID", "vdm-alarm-notifications-" + deviceId),
        1,
        Duration.ofSeconds(10),
        List.of("3A"));

    try (AlarmNotificationStore store = new AlarmNotificationStore(database);
        VdmMqttClient client = new VdmMqttClient(
            config,
            message -> {
              if (!message.suffix().equals("3A")) {
                return;
              }
              try {
                AlarmNotificationStore.Decision decision =
                    store.process(deviceId, message.data());
                System.out.println(MAPPER.writeValueAsString(Map.of(
                    "eventId", message.data().path("eventId").asText(),
                    "action", decision.action(),
                    "notification",
                        decision.notification() == null ? "" : decision.notification(),
                    "reason", decision.reason())));
              } catch (Exception error) {
                throw new IllegalStateException("failed to persist alarm event", error);
              }
            },
            error -> System.err.println("MQTT_ERROR " + error.getMessage()))) {
      client.start();
      System.out.printf(
          "READY deviceId=%s format=%s database=%s pendingNotifications=%d%n",
          deviceId, format, database.toAbsolutePath(), store.pendingNotificationCount());
      new CountDownLatch(1).await();
    }
  }

  private static Path parseDatabase(String[] args) {
    Path database = Path.of("alarm-notifications.sqlite3");
    for (int index = 0; index < args.length; index++) {
      if (!args[index].equals("--database")) {
        throw new IllegalArgumentException("Unknown argument: " + args[index]);
      }
      if (++index >= args.length) {
        throw new IllegalArgumentException("--database requires a value");
      }
      database = Path.of(args[index]);
    }
    return database;
  }

  private static String requiredSetting(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Set " + name);
    }
    return value;
  }

  private static String setting(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
