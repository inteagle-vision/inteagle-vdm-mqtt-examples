package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.*;
import java.time.Duration;
import java.util.*;

/** Small SDK-only recipes. No HTTP or service dependency; explicit --execute sends the request. */
public final class BusinessExamples {
  public record Recipe(String method, Map<String, Object> params) {}

  public static Map<String, Recipe> recipes(PayloadFormat format) {
    return Map.of(
        "device", new Recipe("getAttr", Map.of("keys", List.of("sampleFrequencyHz", "showRoi"))),
        "targets",
            new Recipe("deleteTargets", Map.of("targetIds", List.of("REPLACE_WITH_TARGET_ID"))),
        "measurement", new Recipe("getSyncStatus", Map.of("jobId", "REPLACE_WITH_SYNC_JOB_ID")),
        "alarms",
            new Recipe("listAlarmHistory", Map.of("alarmId", "9007199254740993", "limit", 20)),
        "evidence",
            new Recipe(
                "getEvidenceStatus",
                Map.of(
                    "eventId",
                    "9007199254740993",
                    "kind",
                    format == PayloadFormat.PROTOBUF ? "EVIDENCE_KIND_SNAPSHOT" : "SNAPSHOT")));
  }

  public static void main(String[] args) throws Exception {
    var format =
        PayloadFormat.parse(System.getenv().getOrDefault("VDM_PAYLOAD_FORMAT", "protobuf"));
    var examples = recipes(format);
    var json = new ObjectMapper();
    if (args.length == 0 || args[0].equals("--help")) {
      System.out.println(
          "BusinessExamples <device|targets|measurement|alarms|evidence> [--execute]\n"
              + "Without --execute prints a request. Replace target/job/event IDs in this source"
              + " before sending. targets deletes the named target.");
      return;
    }
    var recipe = examples.get(args[0]);
    if (recipe == null) throw new IllegalArgumentException("unknown recipe");
    System.out.println(json.writeValueAsString(recipe));
    if (args.length < 2 || !args[1].equals("--execute")) return;
    try (var client =
        new VdmMqttClient(
            new VdmMqttClient.Config(
                required("MQTT_HOST"),
                Integer.parseInt(System.getenv().getOrDefault("MQTT_PORT", "1883")),
                VdmTopics.forDevice(required("VDM_DEVICE_ID")),
                format,
                System.getenv("MQTT_USERNAME"),
                System.getenv("MQTT_PASSWORD"),
                null,
                1,
                Duration.ofSeconds(10)),
            null,
            e -> System.err.println(e.getClass().getSimpleName()))) {
      client.start();
      System.out.println(client.call(recipe.method, recipe.params, Duration.ofSeconds(10)).data());
    }
  }

  private static String required(String key) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) throw new IllegalArgumentException("set " + key);
    return value;
  }
}
