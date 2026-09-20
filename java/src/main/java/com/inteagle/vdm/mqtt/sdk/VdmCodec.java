package com.inteagle.vdm.mqtt.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.inteagle.vdm.mqtt.v1.Alarm;
import com.inteagle.vdm.mqtt.v1.Attributes;
import com.inteagle.vdm.mqtt.v1.Event;
import com.inteagle.vdm.mqtt.v1.RpcRequest;
import com.inteagle.vdm.mqtt.v1.RpcResponse;
import com.inteagle.vdm.mqtt.v1.Telemetry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;

/** VDM Topic、JSON/Protobuf 和图片 Payload 编解码。 */
public final class VdmCodec {
  public static final int SCHEMA_VERSION = 1;

  private static final Map<String, String> PUBLIC_RPC_FIELDS = Map.ofEntries(
      Map.entry("listAlarmEvents", "list_alarm_events"),
      Map.entry("syncTelemetry", "sync_telemetry"),
      Map.entry("getSyncStatus", "get_sync_status"),
      Map.entry("cancelSync", "cancel_sync"),
      Map.entry("getAttr", "get_attr"),
      Map.entry("setAttr", "set_attr"),
      Map.entry("reboot", "reboot"),
      Map.entry("syncTime", "sync_time"),
      Map.entry("initRefTargets", "init_ref_targets"),
      Map.entry("addTargets", "add_targets"),
      Map.entry("getTargets", "get_targets"),
      Map.entry("setTargets", "set_targets"),
      Map.entry("deleteTargets", "delete_targets"),
      Map.entry("startMeasurement", "start_measurement"),
      Map.entry("stopMeasurement", "stop_measurement"),
      Map.entry("setLightLevel", "set_light_level"),
      Map.entry("getLightLevel", "get_light_level"),
      Map.entry("snapshot", "snapshot"),
      Map.entry("ispCtl", "isp_ctl"),
      Map.entry("setMotorAngle", "set_motor_angle"),
      Map.entry("getMotorAngle", "get_motor_angle"),
      Map.entry("setMotorZero", "set_motor_zero"),
      Map.entry("enableMotor", "enable_motor"),
      Map.entry("disableMotor", "disable_motor"),
      Map.entry("getCruisePaths", "get_cruise_paths"),
      Map.entry("getEvidenceStatus", "get_evidence_status"),
      Map.entry("retryEvidence", "retry_evidence"),
      Map.entry("ackEvidencePackage", "ack_evidence_package"),
      Map.entry("getAlarmCaps", "get_alarm_caps"),
      Map.entry("listAlarmRules", "list_alarm_rules"),
      Map.entry("applyAlarmRules", "apply_alarm_rules"),
      Map.entry("getAlarmState", "get_alarm_state"),
      Map.entry("listAlarmHistory", "list_alarm_history"));

  private static final Map<Integer, String> RPC_CODE_MESSAGES = Map.ofEntries(
      Map.entry(0, "success"),
      Map.entry(1, "RPC request failed"),
      Map.entry(2, "invalid RPC request"),
      Map.entry(3, "unsupported RPC method"),
      Map.entry(4, "RPC request rate limited"),
      Map.entry(5, "RPC request timed out"),
      Map.entry(6, "resource state changed"),
      Map.entry(100, "resource not found"),
      Map.entry(102, "reference target initialization failed"),
      Map.entry(104, "target lost"),
      Map.entry(200, "measurement not started"),
      Map.entry(201, "measurement already running"),
      Map.entry(300, "motor unavailable"),
      Map.entry(302, "motor moving"),
      Map.entry(303, "motor limit reached"),
      Map.entry(310, "vertical motor unavailable"),
      Map.entry(400, "cruise unavailable"),
      Map.entry(403, "cruise already running"));

  public record RpcEncoding(byte[] payload, String expectedResponseField) {
    public RpcEncoding {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public record ResponseInfo(int reqId, int code, String message, String responseField) {}

  private final PayloadFormat format;
  private final ObjectMapper objectMapper;

  public VdmCodec(PayloadFormat format) {
    this(format, new ObjectMapper());
  }

  public VdmCodec(PayloadFormat format, ObjectMapper objectMapper) {
    if (format == null || objectMapper == null) {
      throw new IllegalArgumentException("format 和 objectMapper 不能为空");
    }
    this.format = format;
    this.objectMapper = objectMapper;
  }

  public PayloadFormat format() {
    return format;
  }

  public DecodedPayload decode(String topic, VdmTopics topics, byte[] payload) throws Exception {
    String suffix = topics.suffix(topic);
    Object value;
    JsonNode data;
    if (suffix.equals("image")) {
      value = decodeImage(payload);
      ObjectNode node = objectMapper.createObjectNode();
      if (value instanceof EvidencePackageChunk chunk) {
        node.put("messageType", chunk.messageType());
        node.put("headerLength", chunk.headerLength());
        node.put("packageFormat", chunk.packageFormat());
        node.put("evidenceKind", chunk.evidenceKind());
        node.put("eventId", Long.toUnsignedString(chunk.eventId()));
        node.put("packageLength", Long.toUnsignedString(chunk.packageLength()));
        node.put("packageSha256", java.util.HexFormat.of().formatHex(chunk.packageSha256()));
        node.put("chunkIndex", chunk.chunkIndex());
        node.put("chunkCount", chunk.chunkCount());
        node.put("chunkOffset", chunk.chunkOffset());
        node.put("chunkBytes", chunk.chunk().length);
      } else {
        ImageFrame image = (ImageFrame) value;
        node.put("version", image.version());
        node.put("headerLength", image.headerLength());
        node.put("sensorId", image.sensorId());
        node.put("imageType", image.imageType());
        node.put("timestampS", image.timestampS());
        node.put("jpegBytes", image.jpeg().length);
      }
      data = node;
    } else if (format == PayloadFormat.JSON) {
      data = objectMapper.readTree(payload);
      if (data == null || !data.isObject()) {
        throw new IllegalArgumentException("JSON 根节点必须是对象");
      }
      value = data;
    } else {
      Message message = parseProtobuf(suffix, payload);
      int version = schemaVersion(message);
      if (version != SCHEMA_VERSION) {
        throw new IllegalArgumentException("不支持 schema_version=" + version);
      }
      value = message;
      data = objectMapper.readTree(JsonFormat.printer().print(message));
    }
    return new DecodedPayload(topic, suffix, payload, value, data);
  }

  public RpcEncoding encodeRpcRequest(String method, Map<String, ?> params, int reqId)
      throws Exception {
    if (reqId == 0) {
      throw new IllegalArgumentException("reqId 必须是非零 signed int32");
    }
    String fieldName = PUBLIC_RPC_FIELDS.get(method);
    if (fieldName == null) {
      throw new IllegalArgumentException("RPC 方法不属于公开 VDM API: " + method);
    }
    Map<String, ?> values = params == null ? Map.of() : params;
    if (format == PayloadFormat.JSON) {
      byte[] payload = objectMapper.writeValueAsBytes(Map.of(
          "reqId", reqId, "method", method, "params", values));
      return new RpcEncoding(payload, fieldName);
    }

    RpcRequest.Builder request = RpcRequest.newBuilder()
        .setSchemaVersion(SCHEMA_VERSION)
        .setReqId(reqId);
    FieldDescriptor field = RpcRequest.getDescriptor().findFieldByName(fieldName);
    if (field == null) {
      throw new IllegalStateException("Schema 缺少 RPC request field: " + fieldName);
    }
    DynamicMessage.Builder body = DynamicMessage.newBuilder(field.getMessageType());
    JsonFormat.parser().merge(objectMapper.writeValueAsString(values), body);
    request.setField(field, body.build());
    return new RpcEncoding(request.build().toByteArray(), fieldName);
  }

  public ResponseInfo responseInfo(Object value) {
    if (value instanceof RpcResponse response) {
      OneofDescriptor responseOneof = RpcResponse.getDescriptor().getOneofs().stream()
          .filter(oneof -> oneof.getName().equals("response"))
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("Schema 缺少 RPC response oneof"));
      FieldDescriptor selected = response.getOneofFieldDescriptor(responseOneof);
      return new ResponseInfo(
          response.getReqId(),
          response.getCode(),
          rpcCodeMessage(response.getCode()),
          selected == null ? null : selected.getName());
    }
    if (!(value instanceof JsonNode node) || !node.isObject()) {
      throw new IllegalArgumentException("RPC 响应类型错误");
    }
    JsonNode reqNode = node.has("reqId") ? node.get("reqId") : node.get("req_id");
    if (reqNode == null || !reqNode.canConvertToInt()) {
      throw new IllegalArgumentException("JSON RPC 响应缺少 reqId");
    }
    int code = parseCode(node.get("code"));
    return new ResponseInfo(
        reqNode.intValue(), code, rpcCodeMessage(code), null);
  }

  private static String rpcCodeMessage(int code) {
    return RPC_CODE_MESSAGES.getOrDefault(code, "RPC request failed (code=" + code + ")");
  }

  private static int parseCode(JsonNode node) {
    if (node == null) {
      return 1;
    }
    if (node.canConvertToInt()) {
      return node.intValue();
    }
    String value = node.asText();
    if(value.equalsIgnoreCase("ok") || value.equalsIgnoreCase("success")) return 0;
    try { return Integer.parseInt(value); } catch(NumberFormatException invalid) { return 1; }
  }

  private static Message parseProtobuf(String suffix, byte[] payload) throws Exception {
    return switch (suffix) {
      case "telemetry" -> Telemetry.parseFrom(payload);
      case "attributes" -> Attributes.parseFrom(payload);
      case "event" -> Event.parseFrom(payload);
      case "3A" -> Alarm.parseFrom(payload);
      case "rpc/req" -> RpcRequest.parseFrom(payload);
      case "rpc/resp" -> RpcResponse.parseFrom(payload);
      default -> throw new IllegalArgumentException("未支持的 Topic: " + suffix);
    };
  }

  private static int schemaVersion(Message message) {
    return switch (message) {
      case Telemetry value -> value.getSchemaVersion();
      case Attributes value -> value.getSchemaVersion();
      case Event value -> value.getSchemaVersion();
      case Alarm value -> value.getSchemaVersion();
      case RpcRequest value -> value.getSchemaVersion();
      case RpcResponse value -> value.getSchemaVersion();
      default -> throw new IllegalArgumentException("Protobuf 消息未声明 schema_version");
    };
  }

  private static Object decodeImage(byte[] payload) {
    if (payload.length > 0 && Byte.toUnsignedInt(payload[0]) == 2) {
      return decodeEvidencePackageChunk(payload);
    }
    if (payload.length < 10) {
      throw new IllegalArgumentException("图片 Payload 小于 VDM Header 与 JPEG 最小长度");
    }
    int headerLength = Byte.toUnsignedInt(payload[1]);
    if (headerLength < 8 || headerLength > payload.length) {
      throw new IllegalArgumentException("非法图片 Header 长度: " + headerLength);
    }
    if (headerLength + 2 > payload.length
        || Byte.toUnsignedInt(payload[headerLength]) != 0xff
        || Byte.toUnsignedInt(payload[headerLength + 1]) != 0xd8) {
      throw new IllegalArgumentException("图片数据不是 JPEG");
    }
    long timestamp = Integer.toUnsignedLong(
        ByteBuffer.wrap(payload, 4, 4).order(ByteOrder.BIG_ENDIAN).getInt());
    return new ImageFrame(
        Byte.toUnsignedInt(payload[0]),
        headerLength,
        Byte.toUnsignedInt(payload[2]),
        Byte.toUnsignedInt(payload[3]),
        timestamp,
        Arrays.copyOfRange(payload, headerLength, payload.length));
  }

  private static EvidencePackageChunk decodeEvidencePackageChunk(byte[] payload) {
    final int headerLength = 76;
    final int chunkBytes = 128 * 1024;
    if (payload.length < headerLength) {
      throw new IllegalArgumentException("告警抓拍图像包 Payload 小于 76 字节固定 Header");
    }
    if (Byte.toUnsignedInt(payload[1]) != headerLength
        || Byte.toUnsignedInt(payload[2]) != 1 || Byte.toUnsignedInt(payload[3]) != 1) {
      throw new IllegalArgumentException("当前只支持 USTAR SNAPSHOT 抓拍图像包");
    }
    ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    long eventId = buffer.getLong(4);
    long packageLength = buffer.getLong(12);
    long chunkIndex = Integer.toUnsignedLong(buffer.getInt(52));
    long chunkCount = Integer.toUnsignedLong(buffer.getInt(56));
    long chunkOffset = buffer.getLong(60);
    long chunkLength = Integer.toUnsignedLong(buffer.getInt(68));
    long flags = Integer.toUnsignedLong(buffer.getInt(72));
    long expectedCount = (packageLength + chunkBytes - 1) / chunkBytes;
    long expectedOffset = chunkIndex * chunkBytes;
    long expectedLength = Math.min(chunkBytes, packageLength - expectedOffset);
    if (eventId == 0 || packageLength < 1 || packageLength > 32L * 1024 * 1024
        || chunkCount != expectedCount || chunkIndex >= chunkCount
        || chunkOffset != expectedOffset || chunkLength != expectedLength
        || payload.length != headerLength + chunkLength || flags != 0) {
      throw new IllegalArgumentException("告警抓拍图像包身份、分块范围、长度或 flags 非法");
    }
    byte[] chunk = Arrays.copyOfRange(payload, headerLength, payload.length);
    if (chunkIndex == 0 && (chunk.length < 262
        || !new String(chunk, 257, 5, java.nio.charset.StandardCharsets.US_ASCII).equals("ustar"))) {
      throw new IllegalArgumentException("告警抓拍图像包首块缺少 USTAR 标识");
    }
    return new EvidencePackageChunk(
        2, headerLength, Byte.toUnsignedInt(payload[2]), Byte.toUnsignedInt(payload[3]),
        eventId, packageLength, Arrays.copyOfRange(payload, 20, 52),
        chunkIndex, chunkCount, chunkOffset, chunk);
  }
}
