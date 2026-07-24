package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.inteagle.vdm.mqtt.v1.Attributes;
import com.inteagle.vdm.mqtt.v1.RpcRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class VdmCodecTest {
  @Test
  void protobufRpcUsesTypedOneof() throws Exception {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmCodec.RpcEncoding encoded = codec.encodeRpcRequest(
        "getAttr", Map.of("keys", List.of("deviceId")), 7);
    RpcRequest request = RpcRequest.parseFrom(encoded.payload());
    assertEquals(1, request.getSchemaVersion());
    assertEquals(7, request.getReqId());
    assertEquals("deviceId", request.getGetAttr().getKeys(0));
    assertEquals("get_attr", encoded.expectedResponseField());
  }

  @Test
  void internalRpcIsRejected() {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    assertThrows(
        IllegalArgumentException.class,
        () -> codec.encodeRpcRequest("wySetAttributes", Map.of(), 8));
  }

  @Test
  void allPublicRpcMethodsBuildTypedBody() throws Exception {
    Set<String> methods = Set.of(
        "getAttr", "setAttr", "reboot", "syncTime", "initRefTargets", "addTargets",
        "getTargets", "setTargets", "deleteTargets", "startMeasurement", "stopMeasurement",
        "setLightLevel", "getLightLevel", "snapshot", "getStorageInfo", "queryTelemetry",
        "uploadS3", "ispCtl", "setMotorAngle", "getMotorAngle", "setMotorZero", "enableMotor",
        "disableMotor", "getCruisePaths", "setCruisePoint", "removeCruisePoint", "startPatrol",
        "stopPatrol", "getPatrolStatus");
    assertEquals(29, methods.size());
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    int reqId = 100;
    for (String method : methods) {
      VdmCodec.RpcEncoding encoded = codec.encodeRpcRequest(method, Map.of(), reqId++);
      RpcRequest request = RpcRequest.parseFrom(encoded.payload());
      assertEquals(encoded.expectedResponseField(), request.getRequestCase().name().toLowerCase());
    }
  }

  @Test
  void protobufDecodeReturnsStrongTypeAndReadableFields() throws Exception {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    Attributes attributes = Attributes.newBuilder()
        .setSchemaVersion(1)
        .setDeviceId("DEMO001")
        .setFirmwareVersion("example-1.0.0")
        .build();
    DecodedPayload decoded = codec.decode(
        topics.topic("attributes"), topics, attributes.toByteArray());
    assertInstanceOf(Attributes.class, decoded.value());
    assertEquals("DEMO001", decoded.data().get("deviceId").asText());
    assertEquals("example-1.0.0", decoded.data().get("firmwareVersion").asText());
  }

  @Test
  void unsupportedSchemaVersionIsRejected() {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    byte[] payload = Attributes.newBuilder().setSchemaVersion(2).build().toByteArray();
    assertThrows(
        IllegalArgumentException.class,
        () -> codec.decode(topics.topic("attributes"), topics, payload));
  }
}
