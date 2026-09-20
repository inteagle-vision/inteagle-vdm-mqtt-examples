package com.inteagle.vdm.service;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.vdm.service.alarms.AlarmService;
import com.inteagle.vdm.service.evidence.EvidenceService;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WebhookTest {
  @TempDir Path dir;

  @Test
  void retriesWithStableIdempotencyKeyAndStopsAtEight() throws Exception {
    var received = new ArrayList<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          received.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(503, -1);
          exchange.close();
        });
    server.start();
    var config =
        new ServiceConfig(
            List.of(
                new ServiceConfig.Broker(
                    "c",
                    "127.0.0.1",
                    1883,
                    null,
                    null,
                    List.of(new ServiceConfig.Device("d", "json", Set.of())))),
            100,
            300,
            100,
            7,
            1048576);
    try (var db = new Store(dir, 100, 300);
        var registry = new DeviceRegistry(config, db, dir)) {
      var rpc = new RpcGateway(registry, config, db);
      var alarms = new AlarmService(db, rpc);
      var evidence = new EvidenceService(db, dir.resolve("evidence"), 1048576);
      try (var workers =
          new Workers(
              db,
              registry,
              config,
              rpc,
              evidence,
              alarms,
              "http://127.0.0.1:" + server.getAddress().getPort(),
              null)) {
        db.alarm(
            "c",
            "d",
            Store.JSON.readTree(
                "{\"eventId\":\"1\",\"alarmId\":\"2\",\"ts\":"
                    + Store.now()
                    + ",\"transition\":\"TRIGGERED\",\"level\":\"ALARM\"}"));
        for (int i = 0; i < 10; i++) {
          workers.outbox();
          db.update("update outbox set next_at=0");
        }
        assertEquals(8, received.size());
        assertEquals(1, new HashSet<>(received).size());
        assertEquals(Store.hash("[\"c\",\"d\",\"1\",\"TRIGGERED\"]"), received.getFirst());
        assertEquals("failed", db.rows("select * from outbox").getFirst().get("status"));
      }
    } finally {
      server.stop(0);
    }
  }
}
