package com.inteagle.vdm.service;

import java.util.*;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.resource.NoResourceFoundException;

@RestControllerAdvice
public class HttpErrors {
  @ExceptionHandler(ApiException.class)
  ResponseEntity<?> api(ApiException error) {
    return ResponseEntity.status(error.status).body(Map.of("error", error.error));
  }

  @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
  ResponseEntity<?> invalid(Exception error) {
    return api(new ApiException(400, "INVALID_ARGUMENT", "invalid request"));
  }

  @ExceptionHandler(NoResourceFoundException.class)
  ResponseEntity<?> missing(Exception error) {
    return api(new ApiException(404, "NOT_FOUND", "route not found"));
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<?> unexpected(Exception error) {
    System.err.println("HTTP failure: " + error.getClass().getSimpleName());
    return api(new ApiException(503, "UNAVAILABLE", "service unavailable"));
  }
}
