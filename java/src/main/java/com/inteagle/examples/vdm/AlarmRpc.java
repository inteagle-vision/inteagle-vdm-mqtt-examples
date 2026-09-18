package com.inteagle.examples.vdm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmCodec;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Run one alarm RPC and optionally print alarm/event messages for a short observation period. */
public final class AlarmRpc {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Path REQUEST_CASES = Path.of("../examples/alarms/requests.json");
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration RPC_TIMEOUT = Duration.ofSeconds(30);

  private record Options(String caseName, Path requestFile, boolean printOnly, Duration listen) {}

  private record Request(String method, Map<String, Object> params) {}

  private AlarmRpc() {}

  public static void main(String[] args) throws Exception {
    Options options = parseOptions(args);
    PayloadFormat format = PayloadFormat.parse(setting("VDM_PAYLOAD_FORMAT", "protobuf"));
    Request request = loadRequest(options, format);
    int requestId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    VdmCodec.RpcEncoding encoded =
        new VdmCodec(format).encodeRpcRequest(request.method(), request.params(), requestId);

    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
        "method", request.method(),
        "params", request.params())));
    System.out.printf(
        "format=%s bytes=%d reqId=%d%n", format, encoded.payload().length, requestId);
    if (options.printOnly()) {
      return;
    }

    String host = requiredSetting("MQTT_HOST");
    String deviceId = requiredSetting("VDM_DEVICE_ID");
    VdmMqttClient.Config config = new VdmMqttClient.Config(
        host,
        Integer.parseInt(setting("MQTT_PORT", "1883")),
        VdmTopics.forDevice(deviceId),
        format,
        System.getenv("MQTT_USERNAME"),
        System.getenv("MQTT_PASSWORD"),
        null,
        1,
        CONNECT_TIMEOUT);

    try (VdmMqttClient client = new VdmMqttClient(
        config,
        message -> {
          if (message.suffix().equals("3A") || message.suffix().equals("event")) {
            System.out.printf("%s %s%n", message.suffix(), message.data());
          }
        },
        error -> System.err.println("MQTT_ERROR " + error.getMessage()))) {
      client.start();
      // Blocking RPC must stay off the MQTT callback thread.
      var response = client.call(
          request.method(), request.params(), requestId, RPC_TIMEOUT, false);
      System.out.println("RESPONSE " + response.data());
      Thread.sleep(options.listen().toMillis());
    }
  }

  private static Options parseOptions(String[] args) {
    String caseName = "capabilities";
    Path requestFile = null;
    boolean printOnly = false;
    Duration listen = Duration.ZERO;
    boolean caseWasSelected = false;

    for (int index = 0; index < args.length; index++) {
      switch (args[index]) {
        case "--print-only" -> printOnly = true;
        case "--case" -> {
          caseName = requiredArgument(args, ++index, "--case");
          caseWasSelected = true;
        }
        case "--request" -> requestFile = Path.of(requiredArgument(args, ++index, "--request"));
        case "--listen" -> {
          long seconds = Long.parseLong(requiredArgument(args, ++index, "--listen"));
          if (seconds < 0) {
            throw new IllegalArgumentException("--listen must be nonnegative");
          }
          listen = Duration.ofSeconds(seconds);
        }
        default -> throw new IllegalArgumentException("Unknown argument: " + args[index]);
      }
    }
    if (caseWasSelected && requestFile != null) {
      throw new IllegalArgumentException("Choose --case or --request, not both");
    }
    return new Options(caseName, requestFile, printOnly, listen);
  }

  private static Request loadRequest(Options options, PayloadFormat format) throws Exception {
    if (options.requestFile() != null) {
      JsonNode request = MAPPER.readTree(options.requestFile().toFile());
      return new Request(
          requiredText(request, "method"),
          MAPPER.convertValue(request.path("params"), new TypeReference<>() {}));
    }

    for (JsonNode item : MAPPER.readTree(REQUEST_CASES.toFile())) {
      if (item.path("name").asText().equals(options.caseName())) {
        return new Request(
            requiredText(item, "method"),
            MAPPER.convertValue(item.path(format.toString()), new TypeReference<>() {}));
      }
    }
    throw new IllegalArgumentException("Unknown case: " + options.caseName());
  }

  private static String requiredText(JsonNode node, String field) {
    String value = node.path(field).asText();
    if (value.isBlank()) {
      throw new IllegalArgumentException("Request is missing " + field);
    }
    return value;
  }

  private static String requiredArgument(String[] args, int index, String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException(option + " requires a value");
    }
    return args[index];
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
