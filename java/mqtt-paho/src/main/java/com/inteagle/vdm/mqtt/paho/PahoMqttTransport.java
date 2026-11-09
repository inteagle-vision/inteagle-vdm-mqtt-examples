package com.inteagle.vdm.mqtt.paho;

import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * 默认 MQTT 3.1.1 传输适配器；Paho 类型被限制在本模块内。
 *
 * <p>使用 clean session、内存持久化和自动重连。每次连接后重订阅，初次启动等待 SUBACK。 本适配器拥有底层连接；close() 终止连接及重连任务，可以重复调用。
 */
public final class PahoMqttTransport implements VdmMqttTransport {
  private final MqttAsyncClient client;
  private final MqttConnectOptions options = new MqttConnectOptions();
  private volatile Listener listener;
  private volatile PahoSubscriptions subscriptions;
  private final Duration timeout;
  private volatile boolean closed;

  /** 只创建连接对象，不发起网络请求；start() 才执行连接和订阅。 */
  public PahoMqttTransport(VdmMqttClient.Config config) throws MqttException {
    timeout = config.connectTimeout();
    client =
        new MqttAsyncClient(
            "tcp://" + config.host() + ":" + config.port(),
            config.clientId(),
            new MemoryPersistence());
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
    options.setCleanSession(true);
    options.setAutomaticReconnect(true);
    options.setConnectionTimeout(
        Math.max(1, (int) Math.min(10, config.connectTimeout().toSeconds())));
    if (config.username() != null && !config.username().isBlank()) {
      options.setUserName(config.username());
      options.setPassword(
          config.password() == null ? new char[0] : config.password().toCharArray());
    }
    client.setCallback(
        new MqttCallbackExtended() {
          @Override
          public void connectComplete(boolean reconnect, String serverURI) {
            if (reconnect && !closed)
              subscriptions
                  .restore(timeout)
                  .exceptionally(
                      error -> {
                        listener.onError(error);
                        return null;
                      });
          }

          @Override
          public void connectionLost(Throwable cause) {
            if (!closed) listener.onDisconnected(cause);
          }

          @Override
          public void messageArrived(String topic, MqttMessage message) {
            listener.onMessage(topic, message.getPayload());
          }

          @Override
          public void deliveryComplete(IMqttDeliveryToken token) {
            // MQTT 投递确认由 publish 的 token 处理，业务 RPC 响应仍由 SDK 独立匹配。
          }
        });
  }

  @Override
  public void start(List<String> topics, int qos, Duration timeout, Listener listener)
      throws Exception {
    if (closed) throw new IllegalStateException("MQTT transport 已关闭");
    this.listener = listener;
    this.subscriptions = new PahoSubscriptions(client, listener, false, topics, qos);
    long deadline = System.nanoTime() + timeout.toNanos();
    // Broker 可能比应用稍晚就绪（例如 Compose 启动）；在总预算内重试。
    // 未完成的连接继续等待同一个 token，不能重复 connect 导致“正在连接”错误。
    IMqttToken connecting = null;
    Throwable lastError = null;
    while (!client.isConnected() && System.nanoTime() < deadline) {
      try {
        if (connecting == null) connecting = client.connect(options);
        long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
        connecting.waitForCompletion(Math.min(500, remainingMs));
      } catch (MqttException error) {
        lastError = error;
        if (error.getReasonCode() == MqttException.REASON_CODE_FAILED_AUTHENTICATION
            || error.getReasonCode() == MqttException.REASON_CODE_NOT_AUTHORIZED) throw error;
        if (connecting == null || connecting.isComplete()) {
          connecting = null;
          long pauseMs = Math.min(200, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
          if (pauseMs > 0) Thread.sleep(pauseMs);
        }
      }
    }
    if (!client.isConnected()) throw new IllegalStateException("连接 MQTT Broker 超时", lastError);
    Duration remaining = Duration.ofNanos(Math.max(1, deadline - System.nanoTime()));
    PahoSubscriptions.await(subscriptions.restore(remaining), remaining);
  }

  @Override
  public void subscribe(String topic, int qos, Duration timeout) throws Exception {
    subscriptions.add(topic, qos, timeout);
  }

  @Override
  public void unsubscribe(String topic, Duration timeout) throws Exception {
    subscriptions.remove(topic, timeout);
  }

  @Override
  public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
      throws Exception {
    client
        .publish(topic, payload, qos, retained)
        .waitForCompletion(Math.max(1, timeout.toMillis()));
  }

  @Override
  public synchronized void close() throws Exception {
    if (closed) return;
    closed = true;
    try {
      if (client.isConnected()) client.disconnect(1_000).waitForCompletion(2_000);
    } finally {
      // 连接失败或断开超时，也要释放 Paho 的网络和定时器线程。
      if (subscriptions != null) subscriptions.close();
      client.close(true);
    }
  }
}
