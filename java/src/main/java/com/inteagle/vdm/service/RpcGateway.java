package com.inteagle.vdm.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.inteagle.vdm.mqtt.sdk.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public final class RpcGateway {
  private final DeviceRegistry registry;
  private final ServiceConfig config;
  private final Store store;
  private final Semaphore slots = new Semaphore(32);

  public RpcGateway(DeviceRegistry registry, ServiceConfig config, Store store) {
    this.registry = registry;
    this.config = config;
    this.store = store;
  }

  public JsonNode call(String c, String d, String method, Map<String, Object> params)
      throws Exception {
    var e = registry.require(c, d);
    try {
      e.client().codec().encodeRpcRequest(method, params, 1);
    } catch (Exception invalid) {
      throw new ApiException(
          400, "INVALID_ARGUMENT", "invalid RPC parameters: " + invalid.getMessage());
    }
    if (!e.client().isConnected())
      throw new ApiException(503, "UNAVAILABLE", "MQTT connection unavailable");
    if (!slots.tryAcquire())
      throw new ApiException(503, "OVERLOADED", "RPC concurrency limit reached");
    try {
      return e.client().call(method, params, Duration.ofMillis(config.rpcTimeoutMs())).data();
    } catch (RpcException error) {
      throw new ApiException(
          502, "DEVICE_ERROR", error.getMessage(), Map.of("deviceCode", error.code()));
    } catch (TimeoutException error) {
      throw new ApiException(
          504,
          "RPC_TIMEOUT",
          "RPC timed out; device outcome unknown",
          Map.of("outcome", "unknown"));
    } catch (ApiException error) {
      throw error;
    } catch (Exception error) {
      throw new ApiException(
          503, "UNAVAILABLE", "MQTT RPC unavailable", Map.of("outcome", "unknown"));
    } finally {
      slots.release();
    }
  }

  public Map<String, Object> execute(
      String c, String d, Operations.Operation op, Map<String, Object> params) throws Exception {
    var e = registry.require(c, d);
    if (!op.capability().equals("-") && !e.device().capabilities().contains(op.capability()))
      throw new ApiException(
          409,
          "UNSUPPORTED_CAPABILITY",
          "operation requires configured " + op.capability() + " capability");
    if (op.method().equals("ackEvidencePackage")
        && !store.verified(
            c,
            d,
            Objects.toString(params.get("eventId"), ""),
            Objects.toString(params.get("packageSha256"), "")))
      throw new ApiException(
          400, "INVALID_ARGUMENT", "evidence ACK requires a matching locally verified package");
    JsonNode response = call(c, d, op.method(), params);
    return Map.of(
        "connectionId",
        c,
        "deviceId",
        d,
        "method",
        op.method(),
        "status",
        op.mode().equals("async") ? "accepted" : "completed",
        "response",
        response);
  }
}
