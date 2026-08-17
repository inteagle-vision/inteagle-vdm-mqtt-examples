package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.inteagle.vdm.mqtt.v1.Attributes;
import com.inteagle.vdm.mqtt.v1.RpcRequest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
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
    methods = new java.util.HashSet<>(methods);
    methods.addAll(Set.of("getEvidenceStatus", "retryEvidence", "ackEvidenceImages"));
    assertEquals(32, methods.size());
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

  @Test
  void decodesStrictEvidenceImageChunk() throws Exception {
    byte[] jpeg = new byte[] {(byte) 0xff, (byte) 0xd8, 'v', 'd', 'm', (byte) 0xff, (byte) 0xd9};
    byte[] payload = new byte[112 + jpeg.length];
    ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    buffer.put(0, (byte) 2).put(1, (byte) 112).put(2, (byte) 1).put(3, (byte) 1);
    buffer.putLong(4, 1_721_805_600_000L);
    buffer.putLong(12, 9001L);
    buffer.putShort(20, (short) 0).putShort(22, (short) 1);
    buffer.putInt(24, -1000).putInt(28, jpeg.length);
    System.arraycopy(MessageDigest.getInstance("SHA-256").digest(jpeg), 0, payload, 32, 32);
    System.arraycopy(MessageDigest.getInstance("SHA-256").digest("manifest".getBytes()), 0, payload, 64, 32);
    buffer.putShort(96, (short) 0).putShort(98, (short) 1);
    buffer.putInt(100, 0).putInt(104, jpeg.length).putInt(108, 0);
    System.arraycopy(jpeg, 0, payload, 112, jpeg.length);

    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    DecodedPayload decoded = codec.decode(topics.topic("image"), topics, payload);
    EvidenceImageChunk chunk = assertInstanceOf(EvidenceImageChunk.class, decoded.value());
    assertEquals(9001L, chunk.eventId());
    assertEquals(-1000, chunk.actualOffsetMs());
    assertEquals(jpeg.length, chunk.chunk().length);
  }
}
