package com.inteagle.vdm.service.measurement;

import com.fasterxml.jackson.databind.JsonNode;
import com.inteagle.vdm.service.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/connections/{c}/devices/{d}/measurement")
public class MeasurementController extends OperationController {
  public MeasurementController(Operations operations, RpcGateway rpc) {
    super(operations, rpc);
  }

  @PostMapping("/{*path}")
  public ResponseEntity<?> operation(
      @PathVariable String c,
      @PathVariable String d,
      HttpServletRequest request,
      @RequestBody(required = false) JsonNode body)
      throws Exception {
    String prefix = "/v1/connections/" + c + "/devices/" + d + "/";
    return execute(c, d, request.getRequestURI().substring(prefix.length()), body);
  }
}
