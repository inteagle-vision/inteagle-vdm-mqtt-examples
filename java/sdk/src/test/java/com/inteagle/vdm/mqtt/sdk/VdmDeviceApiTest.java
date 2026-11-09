package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：验证业务 API 的双格式等价、设备归属、标靶 presence 和拍照回执。 */
class VdmDeviceApiTest {
  private static final String JSON_TARGET =
      """
      {"targetId":"T01","cameraId":0,"roi":{"x":10,"y":20,"width":30,"height":40},
       "actualMeasureDistance":5.5,"role":"MP","targetModel":"VDM-T100",
       "skipMeasurement":true,"initTs":1734567890123}
      """;

  @Test
  void bothFormatsExposeSameBusinessTargetAndSnapshotReceipt() throws Exception {
    for (PayloadFormat format : PayloadFormat.values()) {
      var transport = new DeviceTransport(format);
      var config =
          new VdmMqttClient.Config(
              "unused",
              1883,
              VdmTopics.forDevice("DEVICE_A"),
              format,
              null,
              null,
              null,
              1,
              Duration.ofSeconds(1),
              List.of("attributes", "rpc/resp", "image"));
      try (var client = new VdmMqttClient(config, transport, null, null)) {
        client.start();
        var targets = client.getTargets();
        assertEquals(
            List.of(
                new VdmTarget(
                    "DEVICE_A",
                    "T01",
                    0,
                    new VdmTarget.Roi(10, 20, 30, 40),
                    5.5,
                    VdmTarget.Role.MP,
                    VdmTarget.Model.T100,
                    true,
                    true,
                    1734567890L)),
            targets);
        var receipt = client.requestSnapshot(1, Duration.ofSeconds(1));
        assertEquals("DEVICE_A", receipt.deviceId());
        assertEquals(1, receipt.sensorId());
        assertEquals(1, transport.sensorId);
        assertNotEquals(0, receipt.requestId());
        // 回执没有伪造图片字节；参数错误要在发布请求之前拒绝。
        assertThrows(
            IllegalArgumentException.class,
            () -> client.requestSnapshot(-1, Duration.ofSeconds(1)));
      }
    }
  }

  @Test
  void attributeUpdatesDistinguishMissingEmptyAndTwoDevicesWithSameTargetId() throws Exception {
    var topics = VdmTopics.forDevice("A");
    var codec = new VdmCodec(PayloadFormat.PROTOBUF);
    var absent =
        codec.decode(
            topics.topic("attributes"),
            topics,
            Attributes.newBuilder().setSchemaVersion(1).build().toByteArray());
    var empty =
        codec.decode(
            topics.topic("attributes"),
            topics,
            Attributes.newBuilder()
                .setSchemaVersion(1)
                .setTargetList(TargetList.getDefaultInstance())
                .build()
                .toByteArray());
    assertTrue(VdmTargets.fromAttributes("A", absent).isEmpty());
    assertTrue(VdmTargets.fromAttributes("A", empty).orElseThrow().isEmpty());
    var jsonCodec = new VdmCodec(PayloadFormat.JSON);
    var json =
        jsonCodec.decode(
            topics.topic("attributes"),
            topics,
            ("{\"targets\":[" + JSON_TARGET + "]}")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    assertEquals("A", VdmTargets.fromAttributes("A", json).orElseThrow().get(0).deviceId());
    assertEquals("B", VdmTargets.fromAttributes("B", json).orElseThrow().get(0).deviceId());
    var noList = jsonCodec.decode(topics.topic("attributes"), topics, "{}".getBytes());
    var emptyList =
        jsonCodec.decode(topics.topic("attributes"), topics, "{\"targets\":[]}".getBytes());
    assertTrue(VdmTargets.fromAttributes("A", noList).isEmpty());
    assertTrue(VdmTargets.fromAttributes("A", emptyList).orElseThrow().isEmpty());
  }

  private static class DeviceTransport implements VdmMqttTransport {
    final PayloadFormat format;
    Listener listener;
    int sensorId;

    DeviceTransport(PayloadFormat format) {
      this.format = format;
    }

    public void start(List<String> topics, int qos, Duration timeout, Listener listener) {
      this.listener = listener;
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception {
      byte[] response;
      var mapper = new ObjectMapper();
      if (format == PayloadFormat.PROTOBUF) {
        var request = RpcRequest.parseFrom(payload);
        var builder = RpcResponse.newBuilder().setSchemaVersion(1).setReqId(request.getReqId());
        if (request.hasGetTargets()) {
          builder.setGetTargets(
              GetTargetsResponse.newBuilder()
                  .addTargets(
                      TargetInfo.newBuilder()
                          .setTargetId("T01")
                          .setRoi(Roi.newBuilder().setX(10).setY(20).setWidth(30).setHeight(40))
                          .setDistanceM(5.5f)
                          .setRole(TargetRole.TARGET_ROLE_MP)
                          .setTargetModel(TargetModel.TARGET_MODEL_T100)
                          .setSkipMeasurement(true)
                          .setInitialized(true)
                          .setInitializedAtS(1734567890L)));
        } else {
          sensorId = request.getSnapshot().getSensorId();
          builder.setSnapshot(Empty.getDefaultInstance());
        }
        response = builder.build().toByteArray();
      } else {
        var request = mapper.readTree(payload);
        sensorId = request.path("params").path("sensorId").asInt();
        response =
            mapper.writeValueAsBytes(
                Map.of(
                    "reqId",
                    request.path("reqId").asInt(),
                    "code",
                    0,
                    "data",
                    Map.of("targets", List.of(mapper.readTree(JSON_TARGET)))));
      }
      listener.onMessage(topic.replace("/req", "/resp"), response);
    }

    public void close() {}
  }
}
