package com.inteagle.vdm.service;

import com.inteagle.vdm.service.alarms.AlarmService;
import com.inteagle.vdm.service.evidence.EvidenceService;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class Workers implements AutoCloseable {
  private final Store store;
  private final DeviceRegistry registry;
  private final ServiceConfig config;
  private final RpcGateway rpc;
  private final EvidenceService evidence;
  private final AlarmService alarms;
  private final ScheduledExecutorService workers = Executors.newScheduledThreadPool(4);
  private final HttpClient http =
      HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(5))
          .followRedirects(HttpClient.Redirect.NEVER)
          .build();
  private final String webhook, token;
  public final AtomicLong errors = new AtomicLong();

  public Workers(
      Store store,
      DeviceRegistry registry,
      ServiceConfig config,
      RpcGateway rpc,
      EvidenceService evidence,
      AlarmService alarms,
      String webhook,
      String token) {
    this.store = store;
    this.registry = registry;
    this.config = config;
    this.rpc = rpc;
    this.evidence = evidence;
    this.alarms = alarms;
    this.webhook = webhook;
    this.token = token;
  }

  public void start() {
    workers.scheduleWithFixedDelay(() -> safe(this::inbox), 0, 100, TimeUnit.MILLISECONDS);
    workers.scheduleWithFixedDelay(() -> safe(this::jobs), 0, 250, TimeUnit.MILLISECONDS);
    workers.scheduleWithFixedDelay(() -> safe(this::outbox), 0, 250, TimeUnit.MILLISECONDS);
    workers.scheduleWithFixedDelay(
        () ->
            safe(
                () -> {
                  evidence.prune(config.retentionDays());
                  store.prune(config.retentionDays());
                }),
        60,
        3600,
        TimeUnit.SECONDS);
  }

  private void safe(Store.Work work) {
    try {
      work.run();
    } catch (Exception e) {
      errors.incrementAndGet();
      System.err.println("Worker failure: " + e.getClass().getSimpleName());
    }
  }

  public void inbox() throws Exception {
    for (var row :
        store.rows(
            "select * from inbox where status='pending' and next_at<=? order by id limit 32",
            Store.now())) {
      long id = ((Number) row.get("id")).longValue();
      String c = (String) row.get("c"), d = (String) row.get("d");
      try {
        var entry = registry.require(c, d);
        var message =
            entry
                .client()
                .codec()
                .decode(
                    (String) row.get("topic"),
                    com.inteagle.vdm.mqtt.sdk.VdmTopics.forDevice(d),
                    (byte[]) row.get("payload"));
        switch (message.suffix()) {
          case "telemetry", "attributes" ->
              store.transaction(
                  () -> {
                    store.latest(c, d, message.suffix(), message.data());
                    store.update("update inbox set status='done' where id=?", id);
                  });
          case "3A" -> {
            store.alarm(c, d, message.data());
            store.update("update inbox set status='done' where id=?", id);
          }
          case "event" -> {
            store.event(c, d, message.data());
            store.update("update inbox set status='done' where id=?", id);
          }
          case "image" -> evidence.accept(id, c, d, message.value());
          default -> throw new IllegalArgumentException("unsupported business topic");
        }
      } catch (Exception error) {
        errors.incrementAndGet();
        store.retry("inbox", id, ((Number) row.get("attempts")).intValue() + 1, error);
      }
    }
  }

  public void jobs() throws Exception {
    for (var row :
        store.rows(
            "select * from jobs where status='pending' and next_at<=? order by created limit 8",
            Store.now())) {
      String c = (String) row.get("c"), d = (String) row.get("d");
      try {
        if (row.get("kind").equals("reconcile")) alarms.reconcile(c, d);
        else if (row.get("kind").equals("ack")) {
          var p = Store.JSON.readTree((String) row.get("payload"));
          if (!store.verified(c, d, p.path("eventId").asText(), p.path("packageSha256").asText()))
            throw new IllegalStateException("verified evidence package missing");
          rpc.call(
              c,
              d,
              "ackEvidencePackage",
              Map.of(
                  "eventId",
                  p.path("eventId").asText(),
                  "packageSha256",
                  p.path("packageSha256").asText(),
                  "kind",
                  registry.require(c, d).device().format().equals("protobuf")
                      ? "EVIDENCE_KIND_SNAPSHOT"
                      : "SNAPSHOT"));
        }
        // Do not discard a new reconciliation request written while this RPC was running.
        if (row.get("kind").equals("reconcile")
            && !store
                .rows("select 1 from incidents where c=? and d=? and needs_reconcile=1", c, d)
                .isEmpty())
          throw new IllegalStateException("reconciliation changed during snapshot");
        store.update("update jobs set status='done',error=null where id=?", row.get("id"));
      } catch (Exception error) {
        errors.incrementAndGet();
        store.retry("jobs", row.get("id"), ((Number) row.get("attempts")).intValue() + 1, error);
      }
    }
  }

  public void outbox() throws Exception {
    if (webhook == null || webhook.isBlank()) return;
    for (var row :
        store.rows(
            "select * from outbox where status='pending' and next_at<=? order by created limit 8",
            Store.now())) {
      try {
        var request =
            HttpRequest.newBuilder(URI.create(webhook))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", (String) row.get("id"))
                .POST(HttpRequest.BodyPublishers.ofString((String) row.get("payload")));
        if (token != null && !token.isBlank()) request.header("Authorization", "Bearer " + token);
        var response = http.send(request.build(), HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() < 200 || response.statusCode() >= 300)
          throw new IllegalStateException("webhook HTTP " + response.statusCode());
        store.update("update outbox set status='done',error=null where id=?", row.get("id"));
      } catch (Exception error) {
        errors.incrementAndGet();
        store.retry("outbox", row.get("id"), ((Number) row.get("attempts")).intValue() + 1, error);
      }
    }
  }

  @Override
  public void close() {
    workers.shutdownNow();
    try {
      workers.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
