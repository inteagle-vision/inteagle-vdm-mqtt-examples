package com.inteagle.vdm.mqtt.sdk;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * SDK 解码结果。suffix 是 telemetry、image 等 Topic 后缀。 value 在 Protobuf 模式为生成的消息类、JSON 模式为 JsonNode；图片为
 * ImageFrame 或 EvidencePackageChunk。data 是可读字段视图，图片不含 JPEG 内容，64 位 ID 保留字符串精度。 raw 返回防御性副本；data 为共享
 * JsonNode，多个监听器使用时不要原地修改。
 */
public record DecodedPayload(String topic, String suffix, byte[] raw, Object value, JsonNode data) {
  public DecodedPayload {
    raw = raw.clone();
  }

  @Override
  public byte[] raw() {
    return raw.clone();
  }
}
