package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;

class SharedWireParityTest {
  @Test
  void all33RequestsMatchSharedJsonAndProtobufGoldens() throws Exception {
    var json = new ObjectMapper();
    var cases = json.readTree(Path.of("../tests/fixtures/rpc-cases.json").toFile());
    assertEquals(33, cases.size());
    for (var fixture : cases) {
      String method = fixture.path("method").asText();
      int reqId = fixture.path("reqId").asInt();
      Map<String, Object> jp = json.convertValue(fixture.get("json"), new TypeReference<>() {}),
          pp = json.convertValue(fixture.get("protobuf"), new TypeReference<>() {});
      var encoded = new VdmCodec(PayloadFormat.JSON).encodeRpcRequest(method, jp, reqId);
      var request = json.readTree(encoded.payload());
      assertEquals(method, request.path("method").asText());
      assertEquals(reqId, request.path("reqId").asInt());
      assertEquals(fixture.get("json"), request.get("params"), method);
      assertEquals(
          fixture.path("protobufHex").asText(),
          HexFormat.of()
              .formatHex(
                  new VdmCodec(PayloadFormat.PROTOBUF)
                      .encodeRpcRequest(method, pp, reqId)
                      .payload()),
          method);
    }
  }
}
