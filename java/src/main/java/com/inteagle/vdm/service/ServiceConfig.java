package com.inteagle.vdm.service;

import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public record ServiceConfig(
    List<Broker> connections,
    int rpcTimeoutMs,
    long notificationMaxAgeSeconds,
    int maxInboxRows,
    int retentionDays,
    long maxEvidenceBytes) {
  public record Device(String id, String format, Set<String> capabilities) {}

  public record Broker(
      String id,
      String host,
      int port,
      String usernameEnv,
      String passwordEnv,
      List<Device> devices) {}

  public ServiceConfig {
    if (connections == null || connections.isEmpty())
      throw new IllegalArgumentException("connections required");
    if (rpcTimeoutMs < 100
        || rpcTimeoutMs > 60000
        || maxInboxRows < 1
        || retentionDays < 1
        || maxEvidenceBytes < 1
        || notificationMaxAgeSeconds < 0)
      throw new IllegalArgumentException("invalid service bounds");
    Set<String> ids = new HashSet<>();
    for (var b : connections) {
      validId(b.id);
      if (!ids.add(b.id)) throw new IllegalArgumentException("duplicate connection id");
      if (b.host == null || b.host.isBlank() || b.port < 1 || b.port > 65535)
        throw new IllegalArgumentException("invalid MQTT host/port");
      if (b.devices == null || b.devices.isEmpty())
        throw new IllegalArgumentException("devices required");
      Set<String> ds = new HashSet<>();
      for (var d : b.devices) {
        validId(d.id);
        if (!ds.add(d.id)) throw new IllegalArgumentException("duplicate device");
        if (!Set.of("json", "protobuf").contains(d.format) || d.capabilities == null)
          throw new IllegalArgumentException("invalid device format/capabilities");
      }
    }
  }

  public static ServiceConfig load(Path path) throws Exception {
    return Store.JSON.readValue(path.toFile(), ServiceConfig.class);
  }

  public static void validId(String id) {
    if (id == null || !id.matches("[A-Za-z0-9_-]{1,128}"))
      throw new IllegalArgumentException("invalid id");
  }

  public static void validateBind(String host, String token) throws Exception {
    if (!InetAddress.getByName(host).isLoopbackAddress() && (token == null || token.isBlank()))
      throw new IllegalArgumentException("non-loopback HTTP requires VDM_API_TOKEN");
  }
}
