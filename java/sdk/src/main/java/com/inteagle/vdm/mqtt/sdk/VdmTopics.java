package com.inteagle.vdm.mqtt.sdk;

/** 一个设备的标准 VDM MQTT Topic 集合。 */
public record VdmTopics(String baseTopic) {
  public VdmTopics {
    baseTopic = trimSlashes(baseTopic);
    if (baseTopic.isBlank() || baseTopic.contains("+") || baseTopic.contains("#")) {
      throw new IllegalArgumentException("baseTopic 不能为空且不能包含 MQTT 通配符");
    }
  }

  /** 根据设备 ID 构造 vdm/{deviceId}，拒绝空 ID、路径分隔符和 MQTT 通配符。 */
  public static VdmTopics forDevice(String deviceId) {
    String value = deviceId == null ? "" : deviceId.trim();
    if (value.isBlank() || value.contains("/") || value.contains("+") || value.contains("#")) {
      throw new IllegalArgumentException("deviceId 不能为空且不能包含 MQTT Topic 分隔符或通配符");
    }
    return new VdmTopics("vdm/" + value);
  }

  /** 拼接 Topic 后缀；订阅列表的合法后缀由客户端配置负责校验。 */
  public String topic(String suffix) {
    return baseTopic + "/" + trimSlashes(suffix);
  }

  /** 全设备订阅，仅在确实需要所有数据时使用。 */
  public String wildcard() {
    return topic("#");
  }

  /** 平台下发请求的 Topic，需要发布权限。 */
  public String rpcRequest() {
    return topic("rpc/req");
  }

  /** 设备响应的 Topic，同步 RPC 必须先订阅该 Topic。 */
  public String rpcResponse() {
    return topic("rpc/resp");
  }

  /** 去掉设备前缀；跨设备 Topic 会被拒绝，防止串设备处理。 */
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
