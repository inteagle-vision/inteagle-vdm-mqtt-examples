package com.inteagle.vdm.service;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HttpSecurity extends OncePerRequestFilter {
  private final String token = System.getenv("VDM_API_TOKEN");

  @Override
  protected void doFilterInternal(
      HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    if (!req.getRequestURI().equals("/health")
        && token != null
        && !token.isBlank()
        && !MessageDigest.isEqual(
            ("Bearer " + token).getBytes(StandardCharsets.UTF_8),
            Objects.toString(req.getHeader("Authorization"), "")
                .getBytes(StandardCharsets.UTF_8))) {
      error(res, 401, "UNAUTHORIZED", "bearer token required");
      return;
    }
    if (req.getMethod().equals("POST")) {
      if (req.getContentLengthLong() > 1048576) {
        error(res, 400, "INVALID_ARGUMENT", "body exceeds 1 MiB");
        return;
      }
      byte[] body = req.getInputStream().readNBytes(1048577);
      if (body.length > 1048576) {
        error(res, 400, "INVALID_ARGUMENT", "body exceeds 1 MiB");
        return;
      }
      if (body.length > 0) {
        try {
          Store.JSON
              .reader()
              .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
              .readTree(body);
        } catch (com.fasterxml.jackson.core.JsonProcessingException malformed) {
          error(res, 400, "INVALID_ARGUMENT", "body must contain exactly one valid JSON object");
          return;
        }
      }
      chain.doFilter(
          new HttpServletRequestWrapper(req) {
            @Override
            public ServletInputStream getInputStream() {
              var in = new ByteArrayInputStream(body);
              return new ServletInputStream() {
                public int read() {
                  return in.read();
                }

                public boolean isFinished() {
                  return in.available() == 0;
                }

                public boolean isReady() {
                  return true;
                }

                public void setReadListener(ReadListener listener) {
                  throw new UnsupportedOperationException();
                }
              };
            }

            @Override
            public BufferedReader getReader() {
              return new BufferedReader(
                  new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
            }
          },
          res);
      return;
    }
    chain.doFilter(req, res);
  }

  static void error(HttpServletResponse res, int status, String code, String message)
      throws IOException {
    res.setStatus(status);
    res.setContentType("application/json");
    Store.JSON.writeValue(
        res.getOutputStream(), Map.of("error", Map.of("code", code, "message", message)));
  }
}
