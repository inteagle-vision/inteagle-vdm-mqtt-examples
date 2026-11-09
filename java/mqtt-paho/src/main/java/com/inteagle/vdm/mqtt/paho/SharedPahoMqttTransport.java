package com.inteagle.vdm.mqtt.paho;

import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

/**
 * 复用应用已有的 Paho 异步客户端 Bean，不调用 connect/disconnect/close/setCallback。
 *
 * <p>使用独立订阅回调，因此不覆盖应用的全局回调。传入的 VDM Topic 过滤器必须由本适配器 独占管理，不能和应用其他组件管理完全相同的过滤器；关闭只取消本适配器的订阅。
 *
 * <p>原连接所有者必须从自己的 connectComplete/connectionLost 转发到本适配器， 才能恢复订阅和及时结束在途 RPC。不要把已有连接的自动重连责任交给
 * Starter。
 */
public final class SharedPahoMqttTransport implements VdmMqttTransport {
  private final IMqttAsyncClient client;
  private volatile Listener listener;
  private volatile PahoSubscriptions subscriptions;
  private volatile Duration timeout;
  private volatile boolean closed;

  public SharedPahoMqttTransport(IMqttAsyncClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  @Override
  public void start(List<String> topics, int qos, Duration timeout, Listener listener)
      throws Exception {
    if (closed) throw new IllegalStateException("共享 MQTT 适配器已关闭");
    this.listener = listener;
    this.timeout = timeout;
    this.subscriptions = new PahoSubscriptions(client, listener, true, topics, qos);
    long deadline = System.nanoTime() + timeout.toNanos();
    // 应用负责先启动自己的连接。这里只等待连接可用，不改变其认证、TLS 或会话配置。
    while (!client.isConnected() && System.nanoTime() < deadline) Thread.sleep(20);
    if (!client.isConnected()) throw new TimeoutException("应用提供的 MQTT Bean 尚未连接");
    Duration remaining = Duration.ofNanos(Math.max(1, deadline - System.nanoTime()));
    PahoSubscriptions.await(subscriptions.restore(remaining), remaining);
  }

  /** 在应用现有 MqttCallbackExtended.connectComplete 中调用，恢复 VDM 订阅。 */
  public void connectionRestored() {
    if (!closed && subscriptions != null)
      subscriptions
          .restore(timeout)
          .exceptionally(
              error -> {
                listener.onError(error);
                return null;
              });
  }

  /** 在应用现有 MqttCallback.connectionLost 中调用，让 pending RPC 及时失败。 */
  public void connectionLost(Throwable cause) {
    if (!closed && listener != null) listener.onDisconnected(cause);
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
    if (subscriptions != null) {
      try {
        subscriptions.unsubscribeOwned(Duration.ofSeconds(2));
      } finally {
        subscriptions.close();
      }
    }
    // 不关闭外部 Bean，也不修改应用设置的全局回调、连接参数或重连任务。
  }
}
