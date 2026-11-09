package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.inteagle.vdm.mqtt.v1.Attributes;
import com.inteagle.vdm.mqtt.v1.RpcRequest;
import com.inteagle.vdm.mqtt.v1.RpcResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：验证协议编码、公开 RPC 边界和错误数据拒绝；不是应用接入入口。 */
final class VdmCodecTest {
  @Test
  void absentDisplacementAxisIsEmptyAndDifferentFromMeasuredZero() throws Exception {
    // SDK 维护者回归测试：设备可以只上报 dx/dy，也可以只有 dz；空方向不能被填充为零。
    var series =
        com.inteagle.vdm.mqtt.v1.TargetDisplacementSeries.newBuilder()
            .setTargetId("T01")
            .addDx(1.5f)
            .addDy(2.0f)
            .build();
    var telemetry =
        com.inteagle.vdm.mqtt.v1.Telemetry.newBuilder()
            .setSchemaVersion(1)
            .setDisplacement(
                com.inteagle.vdm.mqtt.v1.DisplacementTelemetry.newBuilder()
                    .addTargets(series)
                    .addTargets(
                        com.inteagle.vdm.mqtt.v1.TargetDisplacementSeries.newBuilder()
                            .setTargetId("T02")
                            .addDz(0.0f)))
            .build();
    var topics = VdmTopics.forDevice("AXES");
    var decoded =
        new VdmCodec(PayloadFormat.PROTOBUF)
            .decode(topics.topic("telemetry"), topics, telemetry.toByteArray());
    var targets =
        ((com.inteagle.vdm.mqtt.v1.Telemetry) decoded.value()).getDisplacement().getTargetsList();
    assertEquals(List.of(), targets.get(0).getDzList());
    assertEquals(0, targets.get(0).getDzCount());
    assertEquals(List.of(0.0f), targets.get(1).getDzList());
    assertEquals(List.of(), targets.get(1).getDxList());
  }

  // getAttr 必须进入对应 oneof 字段，不能退化成无类型的通用字典。
  @Test
  void protobufRpcUsesTypedOneof() throws Exception {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmCodec.RpcEncoding encoded =
        codec.encodeRpcRequest("getAttr", Map.of("keys", List.of("deviceId")), 7);
    RpcRequest request = RpcRequest.parseFrom(encoded.payload());
    assertEquals(1, request.getSchemaVersion());
    assertEquals(7, request.getReqId());
    assertEquals("deviceId", request.getGetAttr().getKeys(0));
    assertEquals("get_attr", encoded.expectedResponseField());
  }

  // 设备内部命令不属于公开 SDK，即使能构造参数也不能编码下发。
  @Test
  void internalRpcIsRejected() {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    assertThrows(
        IllegalArgumentException.class,
        () -> codec.encodeRpcRequest("privateDeviceCommand", Map.of(), 8));
  }

  // 设备错误码映射为本地提示，验证有响应的业务失败与网络失败有所区分。
  @Test
  void rpcErrorTextIsDerivedLocallyFromNumericCode() throws Exception {
    VdmCodec jsonCodec = new VdmCodec(PayloadFormat.JSON);
    VdmCodec.ResponseInfo json =
        jsonCodec.responseInfo(
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"reqId\":8,\"code\":4,\"msg\":\"private device diagnostic\"}"));
    assertEquals("RPC request rate limited", json.message());

    VdmCodec protobufCodec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmCodec.ResponseInfo protobuf =
        protobufCodec.responseInfo(
            RpcResponse.newBuilder()
                .setSchemaVersion(1)
                .setReqId(9)
                .setCode(300)
                .setMessage("private device diagnostic")
                .build());
    assertEquals("motor unavailable", protobuf.message());
  }

  // 遍历公开 RPC 白名单，防止某个方法在协议演进时遗漏其强类型消息体。
  @Test
  void allPublicRpcMethodsBuildTypedBody() throws Exception {
    Set<String> methods =
        Set.of(
            "getAttr",
            "setAttr",
            "reboot",
            "syncTime",
            "initRefTargets",
            "addTargets",
            "getTargets",
            "setTargets",
            "deleteTargets",
            "startMeasurement",
            "stopMeasurement",
            "setLightLevel",
            "getLightLevel",
            "snapshot",
            "ispCtl",
            "setMotorAngle",
            "getMotorAngle",
            "setMotorZero",
            "enableMotor",
            "disableMotor",
            "getCruisePaths");
    methods = new java.util.HashSet<>(methods);
    methods.addAll(
        Set.of(
            "getEvidenceStatus",
            "retryEvidence",
            "ackEvidencePackage",
            "getAlarmCaps",
            "listAlarmRules",
            "applyAlarmRules",
            "getAlarmState",
            "listAlarmHistory"));
    assertEquals(29, methods.size());
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    int reqId = 100;
    for (String method : methods) {
      VdmCodec.RpcEncoding encoded = codec.encodeRpcRequest(method, Map.of(), reqId++);
      RpcRequest request = RpcRequest.parseFrom(encoded.payload());
      assertEquals(encoded.expectedResponseField(), request.getRequestCase().name().toLowerCase());
    }
  }

  // JSON 与 Protobuf 必须执行同一公开方法边界，不能通过切换格式绕过。
  @Test
  void unavailableRpcMethodsAreRejectedInBothFormats() {
    Set<String> unavailable =
        Set.of(
            "getStorageInfo",
            "queryTelemetry",
            "uploadS3",
            "setCruisePoint",
            "removeCruisePoint",
            "startPatrol",
            "stopPatrol",
            "getPatrolStatus");
    for (PayloadFormat format : List.of(PayloadFormat.JSON, PayloadFormat.PROTOBUF)) {
      VdmCodec codec = new VdmCodec(format);
      for (String method : unavailable) {
        assertThrows(
            IllegalArgumentException.class, () -> codec.encodeRpcRequest(method, Map.of(), 200));
      }
    }
  }

  // 同一消息既提供强类型对象，又提供可读字段视图，覆盖应用的两种使用方式。
  @Test
  void protobufDecodeReturnsStrongTypeAndReadableFields() throws Exception {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    Attributes attributes =
        Attributes.newBuilder()
            .setSchemaVersion(1)
            .setDeviceId("DEMO001")
            .setFirmwareVersion("example-1.0.0")
            .build();
    DecodedPayload decoded =
        codec.decode(topics.topic("attributes"), topics, attributes.toByteArray());
    assertInstanceOf(Attributes.class, decoded.value());
    assertEquals("DEMO001", decoded.data().get("deviceId").asText());
    assertEquals("example-1.0.0", decoded.data().get("firmwareVersion").asText());
  }

  // 未知协议版本不能当作当前版本静默解析，以免产生错误业务数据。
  @Test
  void unsupportedSchemaVersionIsRejected() {
    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    byte[] payload = Attributes.newBuilder().setSchemaVersion(2).build().toByteArray();
    assertThrows(
        IllegalArgumentException.class,
        () -> codec.decode(topics.topic("attributes"), topics, payload));
  }

  // 检查证据包 Header、偏移和长度边界，非法分块不能进入落盘重组。
  @Test
  void decodesStrictEvidencePackageChunk() throws Exception {
    byte[] packageBytes = new byte[512];
    System.arraycopy("ustar".getBytes(), 0, packageBytes, 257, 5);
    byte[] payload = new byte[76 + packageBytes.length];
    ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    buffer.put(0, (byte) 2).put(1, (byte) 76).put(2, (byte) 1).put(3, (byte) 1);
    buffer.putLong(4, 9001L).putLong(12, packageBytes.length);
    System.arraycopy(MessageDigest.getInstance("SHA-256").digest(packageBytes), 0, payload, 20, 32);
    buffer.putInt(52, 0).putInt(56, 1).putLong(60, 0).putInt(68, packageBytes.length).putInt(72, 0);
    System.arraycopy(packageBytes, 0, payload, 76, packageBytes.length);

    VdmCodec codec = new VdmCodec(PayloadFormat.PROTOBUF);
    VdmTopics topics = VdmTopics.forDevice("DEMO001");
    DecodedPayload decoded = codec.decode(topics.topic("image"), topics, payload);
    EvidencePackageChunk chunk = assertInstanceOf(EvidencePackageChunk.class, decoded.value());
    assertEquals(9001L, chunk.eventId());
    assertEquals(packageBytes.length, chunk.chunk().length);
  }
}
