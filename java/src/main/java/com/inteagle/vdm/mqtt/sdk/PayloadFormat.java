package com.inteagle.vdm.mqtt.sdk;

import java.util.Locale;

/** 设备连接配置中明确选择的 Payload 格式。 */
public enum PayloadFormat {
  JSON,
  PROTOBUF;

  public static PayloadFormat parse(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Payload 格式不能为空");
    }
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "json" -> JSON;
      case "protobuf" -> PROTOBUF;
      default -> throw new IllegalArgumentException("不支持的 Payload 格式: " + value);
    };
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
