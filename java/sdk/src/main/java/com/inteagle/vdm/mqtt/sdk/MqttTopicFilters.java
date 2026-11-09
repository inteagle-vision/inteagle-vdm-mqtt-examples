package com.inteagle.vdm.mqtt.sdk;

import java.nio.charset.StandardCharsets;

/** MQTT 3.1.1 标准 Topic Filter 校验/匹配；不引入设备专用的订阅语法。 */
public final class MqttTopicFilters {
  private MqttTopicFilters() {}

  public static String validate(String filter) {
    if (filter == null
        || filter.isEmpty()
        || filter.indexOf('\0') >= 0
        || filter.getBytes(StandardCharsets.UTF_8).length > 65535) {
      throw new IllegalArgumentException("MQTT Topic Filter 不能为空、包含 NUL 或超过 65535 字节");
    }
    String[] levels = filter.split("/", -1);
    for (int i = 0; i < levels.length; i++) {
      String level = levels[i];
      if ((level.contains("+") && !level.equals("+"))
          || (level.contains("#") && (!level.equals("#") || i != levels.length - 1))) {
        throw new IllegalArgumentException("MQTT 通配符必须占据完整层级，# 只能位于末层: " + filter);
      }
    }
    return filter;
  }

  public static boolean matches(String filter, String topic) {
    if (topic.startsWith("$") && !filter.startsWith("$")) return false;
    String[] filters = filter.split("/", -1);
    String[] levels = topic.split("/", -1);
    for (int i = 0; i < filters.length; i++) {
      if (filters[i].equals("#")) return true;
      if (i >= levels.length || (!filters[i].equals("+") && !filters[i].equals(levels[i])))
        return false;
    }
    return filters.length == levels.length;
  }
}
