package com.inteagle.vdm.service;

import java.util.*;

public final class ApiException extends RuntimeException {
  public final int status;
  public final Map<String, Object> error;

  public ApiException(int status, String code, String message) {
    this(status, code, message, Map.of());
  }

  public ApiException(int status, String code, String message, Map<String, Object> extra) {
    super(message);
    this.status = status;
    error = new LinkedHashMap<>(Map.of("code", code, "message", message));
    error.putAll(extra);
  }
}
