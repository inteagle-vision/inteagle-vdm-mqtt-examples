package com.inteagle.vdm.mqtt.sdk;

import java.time.Duration;
import java.util.List;

/**
 * MQTT 传输边界，不依赖 Paho、Spring 或数据库。可适配应用已有的 MQTT 客户端。
 *
 * <p>每个实例服务一个 VDM 客户端；实现须支持并发发布，串行投递接收消息， 断线后重新连接并恢复订阅。适配器应在回调返回前保持 payload 有效。
 */
public interface VdmMqttTransport extends AutoCloseable {
  interface Listener {
    /** 原始 MQTT 消息，协议解析由 VdmMqttClient 完成。 */
    void onMessage(String topic, byte[] payload);

    /** 断线后通知客户端结束在途 RPC；重连由传输实现负责。 */
    void onDisconnected(Throwable cause);

    /** 报告连接或订阅失败，不暴露具体客户端类型。 */
    void onError(Throwable error);
  }

  /** 连接并订阅；所有订阅被 Broker 接受后才能返回，失败时抛出异常。 */
  void start(List<String> topics, int qos, Duration timeout, Listener listener) throws Exception;

  /** 运行时增加单个 Filter，等待 SUBACK；失败应回滚新增订阅，重连须恢复成功后的列表。 */
  default void subscribe(String topicFilter, int qos, Duration timeout) throws Exception {
    throw new UnsupportedOperationException("此传输未实现动态订阅");
  }

  /** 取消原样匹配的 Filter，等待 UNSUBACK；不得修改其他组件管理的订阅。 */
  default void unsubscribe(String topicFilter, Duration timeout) throws Exception {
    throw new UnsupportedOperationException("此传输未实现动态取消订阅");
  }

  /** 发布并等待 MQTT 层投递结果；不能把 PUBACK 当作设备 RPC 的业务响应。 */
  void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
      throws Exception;

  /** 幂等停止重连、释放本适配器拥有的资源；不得关闭不归它所有的共享连接。 */
  @Override
  void close() throws Exception;
}
