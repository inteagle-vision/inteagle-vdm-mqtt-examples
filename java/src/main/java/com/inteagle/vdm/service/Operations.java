package com.inteagle.vdm.service;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.*;

public final class Operations {
  public record Operation(
      String route, String method, String module, String mode, String capability) {}

  private final Map<String, Operation> routes = new LinkedHashMap<>();

  public Operations() throws Exception {
    try (var in = getClass().getResourceAsStream("/contracts/operations.json")) {
      if (in == null) throw new IllegalStateException("operations contract missing");
      for (Operation op : Store.JSON.readValue(in, new TypeReference<List<Operation>>() {}))
        routes.put(op.route, op);
    }
  }

  public Operation require(String route) {
    var op = routes.get(route);
    if (op == null) throw new ApiException(404, "NOT_FOUND", "unknown operation");
    return op;
  }

  public Collection<Operation> all() {
    return routes.values();
  }
}
