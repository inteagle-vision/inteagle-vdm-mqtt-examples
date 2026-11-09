package com.inteagle.vdm.mqtt.spring;

import com.inteagle.vdm.mqtt.sdk.DecodedPayload;

/**
 * 注册为 Spring Bean 即可接收已解码的数据，不需要实现连接、订阅或协议解析。
 *
 * <p>回调按设备 ID 分配到固定业务线程，同一设备按接收顺序执行，不同分区可并行。 同一个监听器 Bean 可能同时收到不同设备的回调，其共享状态必须保证线程安全。 可以调用同步
 * RPC；耗时任务应交给自己的持久化队列。异常会被记录，不自动重试。
 */
@FunctionalInterface
public interface VdmMessageListener {
  /** deviceId 从当前消息的 vdm/{deviceId}/... Topic 提取；value 保留 Protobuf 或图片强类型对象。 */
  void onMessage(String deviceId, DecodedPayload message) throws Exception;
}
