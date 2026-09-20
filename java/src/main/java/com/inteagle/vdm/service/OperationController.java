package com.inteagle.vdm.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.http.ResponseEntity;

/** Shared validation; concrete business controllers expose only their contract routes. */
public abstract class OperationController {
  protected final Operations operations;
  protected final RpcGateway rpc;

  protected OperationController(Operations operations, RpcGateway rpc) {
    this.operations = operations;
    this.rpc = rpc;
  }

  protected ResponseEntity<?> execute(String c, String d, String route, JsonNode body)
      throws Exception {
    var op = operations.require(route);
    Map<String, Object> params = Map.of();
    if (body != null) {
      if (!body.isObject())
        throw new ApiException(400, "INVALID_ARGUMENT", "body must be an object");
      var names = body.fieldNames();
      while (names.hasNext())
        if (!names.next().equals("params"))
          throw new ApiException(400, "INVALID_ARGUMENT", "unknown request field");
      if (body.has("params")) {
        if (!body.get("params").isObject())
          throw new ApiException(400, "INVALID_ARGUMENT", "params must be an object");
        params = Store.JSON.convertValue(body.get("params"), new TypeReference<>() {});
      }
    }
    var result = rpc.execute(c, d, op, params);
    return ResponseEntity.status(op.mode().equals("async") ? 202 : 200).body(result);
  }
}
