package com.inteagle.vdm.mqtt.spring;

import com.inteagle.vdm.mqtt.sdk.VdmMqttGateway;
import org.springframework.context.SmartLifecycle;

/** 所有业务 Bean 创建完成后再连接 Broker；启动失败也释放 Paho 的连接资源。 */
final class VdmMqttLifecycle implements SmartLifecycle {
  private final VdmMqttGateway client;
  private final boolean autoStartup;
  private final VdmMessageDispatcher dispatcher;
  private volatile boolean running;

  VdmMqttLifecycle(VdmMqttGateway client, boolean autoStartup, VdmMessageDispatcher dispatcher) {
    this.client = client;
    this.autoStartup = autoStartup;
    this.dispatcher = dispatcher;
  }

  @Override
  public synchronized void start() {
    if (running) return;
    try {
      client.start();
      running = true;
    } catch (Exception error) {
      if (error instanceof InterruptedException) Thread.currentThread().interrupt();
      try {
        client.close();
      } catch (Exception closeError) {
        error.addSuppressed(closeError);
      }
      throw new IllegalStateException("VDM MQTT 启动失败，请检查 Broker、认证和订阅权限", error);
    }
  }

  @Override
  public synchronized void stop() {
    try {
      client.close();
    } catch (Exception error) {
      throw new IllegalStateException("VDM MQTT 关闭失败", error);
    } finally {
      // 必须在 Spring 销毁业务监听器（例如关闭 SQLite）前处理完已入队消息。
      dispatcher.close();
      running = false;
    }
  }

  @Override
  public boolean isRunning() {
    // auto-startup=false 时应用可能直接调用 gateway.start()，仍参与容器的停机阶段。
    return running || client.isRunning();
  }

  @Override
  public boolean isAutoStartup() {
    return autoStartup;
  }
}
