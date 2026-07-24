package com.inteagle.vdm.mqtt.sdk;

/** 一个设备的标准 VDM MQTT Topic 集合。 */
public record VdmTopics(String baseTopic) {
  public VdmTopics {
    baseTopic = trimSlashes(baseTopic);
    if (baseTopic.isBlank() || baseTopic.contains("+") || baseTopic.contains("#")) {
      throw new IllegalArgumentException("baseTopic 不能为空且不能包含 MQTT 通配符");
    }
  }

  public static VdmTopics forDevice(String deviceId) {
    String value = deviceId == null ? "" : deviceId.trim();
    if (value.isBlank() || value.contains("/") || value.contains("+") || value.contains("#")) {
      throw new IllegalArgumentException("deviceId 不能为空且不能包含 MQTT Topic 分隔符或通配符");
    }
    return new VdmTopics("vdm/" + value);
  }

  public String topic(String suffix) {
    return baseTopic + "/" + trimSlashes(suffix);
  }

  public String wildcard() {
    return topic("#");
  }

  public String rpcRequest() {
    return topic("rpc/req");
  }

  public String rpcResponse() {
    return topic("rpc/resp");
  }

  public String suffix(String topic) {
    String prefix = baseTopic + "/";
    if (!topic.startsWith(prefix)) {
      throw new IllegalArgumentException("Topic 不属于当前设备: " + topic);
    }
    return topic.substring(prefix.length());
  }

  private static String trimSlashes(String value) {
    if (value == null) {
      return "";
    }
    int start = 0;
    int end = value.length();
    while (start < end && value.charAt(start) == '/') {
      start++;
    }
    while (end > start && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(start, end);
  }
}
