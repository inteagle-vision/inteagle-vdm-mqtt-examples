package com.inteagle.vdm.mqtt.spring;

import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** application.yml 中的 vdm.mqtt 配置；默认仅订阅遥测，不附带告警、图片或 RPC。 */
@ConfigurationProperties("vdm.mqtt")
public class VdmMqttProperties {
  /** 关闭后不创建客户端和后台线程，可用于无需接入设备的部署环境。 */
  private boolean enabled = true;

  /** Broker 主机名或 IP，不含 tcp:// 前缀。当前使用 MQTT TCP。 */
  private String host;

  private int port = 1883;
  private String username;
  private String password;

  /** 仅单设备接入时填写；与 device-ids/all-devices 互斥，平台多设备接入不填写本项。 */
  private String deviceId;

  /** 多设备时填写列表，所有设备共享一个 MQTT 连接，与 device-id/all-devices 互斥。 */
  private List<String> deviceIds = List.of();

  /** 平台账号订阅 vdm/+/...；必须显式开启，不作为默认行为。 */
  private boolean allDevices;

  /** MQTT 标准 Topic Filter，与设备选择模式互斥；显式空列表允许启动后动态订阅。 */
  private List<String> topicFilters;

  /** 必须与设备的 MQTT 连接配置一致。 */
  private PayloadFormat payloadFormat = PayloadFormat.PROTOBUF;

  /** 留空时生成随机 ID，避免多个应用实例在 Broker 上互相踢下线。 */
  private String clientId;

  private int qos = 1;
  private Duration connectTimeout = Duration.ofSeconds(10);

  /** Topic 后缀；图片填 image，RPC 必须显式增加 rpc/resp。 */
  private List<String> subscriptions = List.of("telemetry");

  /** false 时仅创建客户端，适用于离线测试；业务自行调用 start()。 */
  private boolean autoStartup = true;

  /** 固定业务分区数；按设备 ID 分配，同设备有序、不同分区并行，不按设备创建线程。 */
  private int listenerConcurrency = 4;

  /** 每个业务分区的队列容量；满时明确记录拒绝错误，不在 MQTT 接收线程执行业务。 */
  private int listenerQueueCapacity = 256;

  VdmMqttClient.Config clientConfig() {
    if (connectTimeout == null || connectTimeout.toMillis() < 1) {
      throw new IllegalArgumentException("vdm.mqtt.connect-timeout 必须至少为 1ms");
    }
    if (listenerQueueCapacity < 1) {
      throw new IllegalArgumentException("vdm.mqtt.listener-queue-capacity 必须大于 0");
    }
    if (listenerConcurrency < 1) {
      throw new IllegalArgumentException("vdm.mqtt.listener-concurrency 必须大于 0");
    }
    if (subscriptions == null || subscriptions.isEmpty()) {
      throw new IllegalArgumentException("vdm.mqtt.subscriptions 至少需要一个 Topic 后缀");
    }
    List<String> selected = selectedDeviceIds();
    return new VdmMqttClient.Config(
        host == null ? "localhost" : host,
        port,
        VdmTopics.forDevice(selected.isEmpty() ? "ALL" : selected.get(0)),
        payloadFormat,
        username,
        password,
        clientId,
        qos,
        connectTimeout,
        subscriptions);
  }

  List<String> selectedDeviceIds() {
    boolean single = deviceId != null && !deviceId.isBlank();
    boolean multiple = deviceIds != null && !deviceIds.isEmpty();
    if ((single ? 1 : 0)
            + (multiple ? 1 : 0)
            + (allDevices ? 1 : 0)
            + (topicFilters != null ? 1 : 0)
        != 1) {
      throw new IllegalArgumentException(
          "device-id、device-ids、all-devices、topic-filters 必须且只能选择一项");
    }
    if (topicFilters != null)
      topicFilters.forEach(com.inteagle.vdm.mqtt.sdk.MqttTopicFilters::validate);
    List<String> selected =
        single ? List.of(deviceId) : multiple ? List.copyOf(deviceIds) : List.of();
    selected.forEach(VdmTopics::forDevice);
    return selected;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public List<String> getDeviceIds() {
    return deviceIds;
  }

  public void setDeviceIds(List<String> deviceIds) {
    this.deviceIds = deviceIds;
  }

  public boolean isAllDevices() {
    return allDevices;
  }

  public void setAllDevices(boolean allDevices) {
    this.allDevices = allDevices;
  }

  public List<String> getTopicFilters() {
    return topicFilters;
  }

  public void setTopicFilters(List<String> topicFilters) {
    this.topicFilters = topicFilters;
  }

  public PayloadFormat getPayloadFormat() {
    return payloadFormat;
  }

  public void setPayloadFormat(PayloadFormat payloadFormat) {
    this.payloadFormat = payloadFormat;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public int getQos() {
    return qos;
  }

  public void setQos(int qos) {
    this.qos = qos;
  }

  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(Duration connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public List<String> getSubscriptions() {
    return subscriptions;
  }

  public void setSubscriptions(List<String> subscriptions) {
    this.subscriptions = subscriptions;
  }

  public boolean isAutoStartup() {
    return autoStartup;
  }

  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  public int getListenerQueueCapacity() {
    return listenerQueueCapacity;
  }

  public void setListenerQueueCapacity(int capacity) {
    this.listenerQueueCapacity = capacity;
  }

  public int getListenerConcurrency() {
    return listenerConcurrency;
  }

  public void setListenerConcurrency(int concurrency) {
    this.listenerConcurrency = concurrency;
  }
}
