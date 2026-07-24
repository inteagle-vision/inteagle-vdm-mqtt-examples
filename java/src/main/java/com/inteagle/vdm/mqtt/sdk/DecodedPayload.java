package com.inteagle.vdm.mqtt.sdk;

import com.fasterxml.jackson.databind.JsonNode;

/** SDK 解码结果：value 是强类型对象，data 是可读的 JSON 字段视图。 */
public record DecodedPayload(
    String topic,
    String suffix,
    byte[] raw,
    Object value,
    JsonNode data) {
  public DecodedPayload {
    raw = raw.clone();
  }

  @Override
  public byte[] raw() {
    return raw.clone();
  }
}
