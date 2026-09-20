package com.inteagle.vdm.service;

import com.inteagle.vdm.service.alarms.AlarmService;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController
public class ReadController {
  private final DeviceRegistry registry;
  private final Store store;
  private final Workers workers;
  private final AlarmService alarms;

  public ReadController(
      DeviceRegistry registry, Store store, Workers workers, AlarmService alarms) {
    this.registry = registry;
    this.store = store;
    this.workers = workers;
    this.alarms = alarms;
  }

  @GetMapping("/health")
  public Object health() throws Exception {
    var connections =
        registry.all().stream()
            .map(
                e ->
                    Map.of(
                        "connectionId",
                        e.connectionId(),
                        "deviceId",
                        e.device().id(),
                        "connected",
                        e.client().isConnected()))
            .toList();
    var counters = store.counters();
    counters.put("mqttErrors", registry.mqttErrors.get());
    counters.put("workerErrors", workers.errors.get());
    boolean degraded =
        registry.all().stream().anyMatch(e -> !e.client().isConnected())
            || counters.entrySet().stream()
                .anyMatch(
                    e -> e.getKey().endsWith("_failed") && ((Number) e.getValue()).longValue() > 0);
    return Map.of(
        "status", degraded ? "degraded" : "ok", "connections", connections, "counters", counters);
  }

  @GetMapping("/v1/devices")
  public Object devices() {
    return Map.of(
        "devices",
        registry.all().stream()
            .map(
                e ->
                    Map.of(
                        "connectionId",
                        e.connectionId(),
                        "deviceId",
                        e.device().id(),
                        "format",
                        e.device().format(),
                        "capabilities",
                        e.device().capabilities()))
            .toList());
  }

  @GetMapping("/v1/connections/{c}/devices/{d}/latest")
  public Object latest(@PathVariable String c, @PathVariable String d) throws Exception {
    registry.require(c, d);
    return store.latest(c, d);
  }

  @GetMapping("/v1/connections/{c}/devices/{d}/alarms/local")
  public Object alarms(@PathVariable String c, @PathVariable String d) throws Exception {
    registry.require(c, d);
    return Map.of("items", alarms.local(c, d));
  }
}
