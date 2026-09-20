package com.inteagle.vdm.service;

import com.inteagle.vdm.mqtt.sdk.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** A distinct SDK and RPC pending map for every configured broker/device tuple. */
public final class DeviceRegistry implements AutoCloseable {
  public record Entry(String connectionId, ServiceConfig.Device device, VdmMqttClient client) {}

  private final Map<String, Entry> entries = new LinkedHashMap<>();
  private final ScheduledExecutorService reconnect = Executors.newScheduledThreadPool(2);
  public final AtomicLong mqttErrors = new AtomicLong();

  public DeviceRegistry(ServiceConfig config, Store store, Path data) throws Exception {
    for (var b : config.connections())
      for (var d : b.devices()) {
        String key = key(b.id(), d.id());
        Path persistence = data.resolve("mqtt").resolve(b.id()).resolve(d.id());
        Files.createDirectories(persistence);
        var mqtt =
            new VdmMqttClient(
                new VdmMqttClient.Config(
                    b.host(),
                    b.port(),
                    VdmTopics.forDevice(d.id()),
                    PayloadFormat.parse(d.format()),
                    env(b.usernameEnv()),
                    env(b.passwordEnv()),
                    "vdm-java-" + Store.hash(key).substring(0, 24),
                    1,
                    Duration.ofSeconds(4),
                    List.of("telemetry", "attributes", "event", "3A", "image", "rpc/resp")),
                null,
                error -> {
                  mqttErrors.incrementAndGet();
                  System.err.println(
                      "MQTT delivery/connection failure ["
                          + b.id()
                          + "/"
                          + d.id()
                          + "] "
                          + error.getClass().getSimpleName());
                },
                (topic, payload) -> store.enqueue(b.id(), d.id(), topic, payload),
                persistence);
        entries.put(key, new Entry(b.id(), d, mqtt));
      }
  }

  public void start() {
    for (var e : entries.values())
      reconnect.scheduleWithFixedDelay(
          () -> {
            if (!e.client.isConnected())
              try {
                e.client.start();
              } catch (Exception failure) {
                mqttErrors.incrementAndGet();
              }
          },
          0,
          5,
          TimeUnit.SECONDS);
  }

  public Entry require(String c, String d) {
    var e = entries.get(key(c, d));
    if (e == null) throw new ApiException(404, "NOT_FOUND", "unknown connection/device");
    return e;
  }

  public Collection<Entry> all() {
    return Collections.unmodifiableCollection(entries.values());
  }

  private static String key(String c, String d) {
    return c + "/" + d;
  }

  private static String env(String name) {
    return name == null || name.isBlank() ? null : System.getenv(name);
  }

  @Override
  public void close() {
    reconnect.shutdownNow();
    for (var e : entries.values())
      try {
        e.client.close();
      } catch (Exception ignored) {
      }
  }
}
