package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inteagle.vdm.mqtt.v1.Alarm;
import com.inteagle.vdm.mqtt.v1.RpcRequest;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class AlarmContractTest {
  private final ObjectMapper mapper = new ObjectMapper();
  @Test void configurationExamples() throws Exception {
    var cases = mapper.readTree(Path.of("../examples/alarms/requests.json").toFile());
    for (var item : cases) {
      for (var format : PayloadFormat.values()) {
        Map<String, Object> params = mapper.convertValue(item.get(format.toString()), new TypeReference<>() {});
        String method = item.get("method").asText();
        var encoded = new VdmCodec(format).encodeRpcRequest(method, params, 91);
        if (format == PayloadFormat.PROTOBUF) {
          ObjectNode want = mapper.createObjectNode().put("schemaVersion",1).put("reqId",91);
          want.set(method,item.get("protobuf"));
          var builder = RpcRequest.newBuilder();
          JsonFormat.parser().merge(want.toString(),builder);
          assertEquals(builder.build(),RpcRequest.parseFrom(encoded.payload()),item.get("name").asText());
        } else {
          assertEquals(item.get("json"),mapper.readTree(encoded.payload()).get("params"));
        }
      }
    }
  }
  @Test void lifecycleExamples() throws Exception {
    var topics = VdmTopics.forDevice("DEMO001");
    var cases = mapper.readTree(Path.of("../examples/alarms/events.json").toFile());
    for (var item : cases) {
      for (var format : PayloadFormat.values()) {
        var value = item.get(format.toString());
        byte[] raw = mapper.writeValueAsBytes(value);
        if (format == PayloadFormat.PROTOBUF) {
          var builder = Alarm.newBuilder();
          JsonFormat.parser().merge(value.toString(),builder);
          raw = builder.build().toByteArray();
        }
        var decoded = new VdmCodec(format).decode(topics.topic("3A"),topics,raw);
        assertEquals(value.get("eventId"),decoded.data().get("eventId"));
        assertEquals(value.get("alarmId"),decoded.data().get("alarmId"));
        assertEquals(value.has("level"),decoded.data().has("level"));
        if (decoded.value() instanceof Alarm alarm) assertEquals(value.has("level"),alarm.hasLevel());
      }
    }
  }
}
