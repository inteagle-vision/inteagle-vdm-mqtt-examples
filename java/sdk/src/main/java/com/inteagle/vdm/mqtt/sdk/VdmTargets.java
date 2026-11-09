package com.inteagle.vdm.mqtt.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.inteagle.vdm.mqtt.v1.Attributes;
import com.inteagle.vdm.mqtt.v1.RpcResponse;
import com.inteagle.vdm.mqtt.v1.TargetInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** 统一 getTargets 响应和 attributes 标靶列表，业务不需要重复处理 JSON/Protobuf 差异。 */
public final class VdmTargets {
  private VdmTargets() {}

  /**
   * 属性未携带 targets 时返回 Optional.empty（不改变现有列表）；明确空列表返回 Optional.of(emptyList)。
   * 不能把属性增量消息当成完整设备状态覆盖。deviceId 必须来自可信 Topic 路由，而非 Payload 自报值。
   */
  public static Optional<List<VdmTarget>> fromAttributes(String deviceId, DecodedPayload message) {
    if (!message.suffix().equals("attributes")) return Optional.empty();
    if (message.value() instanceof Attributes attributes) {
      if (!attributes.hasTargetList()) return Optional.empty();
      return Optional.of(
          attributes.getTargetList().getTargetsList().stream()
              .map(t -> fromProto(deviceId, t))
              .toList());
    }
    if (!message.data().has("targets")) return Optional.empty();
    return Optional.of(fromJson(deviceId, message.data().get("targets")));
  }

  static List<VdmTarget> fromResponse(String deviceId, DecodedPayload response) {
    if (response.value() instanceof RpcResponse rpc) {
      if (!rpc.hasGetTargets()) throw new IllegalArgumentException("响应缺少 getTargets 消息体");
      return rpc.getGetTargets().getTargetsList().stream()
          .map(t -> fromProto(deviceId, t))
          .toList();
    }
    JsonNode data = response.data().path("data");
    return fromJson(deviceId, data.isArray() ? data : data.path("targets"));
  }

  private static VdmTarget fromProto(String deviceId, TargetInfo target) {
    var roi =
        target.hasRoi()
            ? new VdmTarget.Roi(
                Integer.toUnsignedLong(target.getRoi().getX()),
                Integer.toUnsignedLong(target.getRoi().getY()),
                Integer.toUnsignedLong(target.getRoi().getWidth()),
                Integer.toUnsignedLong(target.getRoi().getHeight()))
            : null;
    return new VdmTarget(
        deviceId,
        target.getTargetId(),
        target.getSensorId(),
        roi,
        target.getDistanceM(),
        role(target.getRole().name().replace("TARGET_ROLE_", "")),
        model(target.getTargetModel().name().replace("TARGET_MODEL_", "")),
        target.getSkipMeasurement(),
        target.getInitialized(),
        target.hasInitializedAtS() ? target.getInitializedAtS() : null);
  }

  private static List<VdmTarget> fromJson(String deviceId, JsonNode values) {
    if (values == null || !values.isArray()) throw new IllegalArgumentException("targets 必须为数组");
    var targets = new ArrayList<VdmTarget>();
    for (JsonNode target : values) {
      String id = field(target, "targetId", "target_id").asText("");
      if (id.isBlank()) throw new IllegalArgumentException("标靶缺少 targetId");
      JsonNode r = target.path("roi");
      var roi =
          r.isObject()
              ? new VdmTarget.Roi(
                  r.path("x").asLong(),
                  r.path("y").asLong(),
                  field(r, "width", "w").asLong(),
                  field(r, "height", "h").asLong())
              : null;
      JsonNode timestamp = field(target, "initializedAt", "initialized_at", "initTs", "init_ts");
      Long initializedAt = timestamp.isMissingNode() ? null : timestamp.asLong();
      // 与设备公开编码规则一致：旧 DTO 的 initTs 可能以毫秒表示，SDK 对外统一秒。
      if (initializedAt != null && initializedAt >= 1_000_000_000_000L) initializedAt /= 1000;
      targets.add(
          new VdmTarget(
              deviceId,
              id,
              field(target, "sensorId", "cameraId", "camera_id").asInt(),
              roi,
              field(target, "distance", "actualMeasureDistance").asDouble(),
              role(field(target, "role", "targetRole").asText("")),
              model(field(target, "targetModel", "target_model").asText("")),
              field(target, "skipMeasurement", "skip_measurement").asBoolean(),
              field(target, "initialized").asBoolean(initializedAt != null),
              initializedAt));
    }
    return List.copyOf(targets);
  }

  private static JsonNode field(JsonNode node, String... names) {
    for (String name : names) if (node.hasNonNull(name)) return node.get(name);
    return com.fasterxml.jackson.databind.node.MissingNode.getInstance();
  }

  private static VdmTarget.Role role(String name) {
    try {
      return VdmTarget.Role.valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ignored) {
      return VdmTarget.Role.UNSPECIFIED;
    }
  }

  private static VdmTarget.Model model(String name) {
    try {
      return VdmTarget.Model.valueOf(name.toUpperCase(Locale.ROOT).replaceFirst("^VDM-", ""));
    } catch (IllegalArgumentException ignored) {
      return VdmTarget.Model.UNSPECIFIED;
    }
  }
}
