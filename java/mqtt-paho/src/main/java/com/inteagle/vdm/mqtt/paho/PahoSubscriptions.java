package com.inteagle.vdm.mqtt.paho;

import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;

/**
 * 两种 Paho 适配器共用的订阅管理。重连和运行时变更串行执行，避免旧快照覆盖新订阅。
 *
 * <p>绝不在 Paho 回调线程等待 SUBACK/UNSUBACK；该线程必须继续接收确认与设备消息。
 */
final class PahoSubscriptions implements AutoCloseable {
  private final IMqttAsyncClient client;
  private final VdmMqttTransport.Listener listener;
  private final boolean perTopicListener;
  private final LinkedHashMap<String, Integer> desired = new LinkedHashMap<>();
  private final LinkedHashSet<String> cleanup = new LinkedHashSet<>();
  private final ExecutorService worker =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "vdm-mqtt-subscriptions");
            thread.setDaemon(true);
            return thread;
          });

  PahoSubscriptions(
      IMqttAsyncClient client,
      VdmMqttTransport.Listener listener,
      boolean perTopicListener,
      List<String> topics,
      int qos) {
    this.client = client;
    this.listener = listener;
    this.perTopicListener = perTopicListener;
    topics.forEach(topic -> desired.put(topic, qos));
  }

  CompletableFuture<Void> restore(Duration timeout) {
    return submit(result -> restoreBroker(timeout));
  }

  private void restoreBroker(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    // 共享持久会话可能保留上一次失败操作的订阅，恢复前先清理。
    for (String topic : List.copyOf(cleanup)) {
      client.unsubscribe(topic).waitForCompletion(remaining(deadline));
      cleanup.remove(topic);
    }
    for (var entry : desired.entrySet())
      subscribeBroker(entry.getKey(), entry.getValue(), deadline);
  }

  void add(String topic, int qos, Duration timeout) throws Exception {
    await(
        submit(
            result -> {
              if (desired.containsKey(topic)) return;
              try {
                subscribeBroker(topic, qos, System.nanoTime() + timeout.toNanos());
                // 完成和调用方超时竞争同一个结果，只有调用方能观察到成功时才提交期望状态。
                if (result.complete(null)) {
                  desired.put(topic, qos);
                  cleanup.remove(topic);
                } else {
                  rollbackAddition(topic, timeout);
                }
              } catch (Exception error) {
                // 超时并不表示 Broker 未执行；补发取消，断线时留待下一次恢复处理。
                try {
                  rollbackAddition(topic, timeout);
                } catch (Exception rollbackError) {
                  error.addSuppressed(rollbackError);
                }
                throw error;
              }
            }),
        timeout.multipliedBy(2).plusSeconds(1));
  }

  private void rollbackAddition(String topic, Duration timeout) throws Exception {
    cleanup.add(topic);
    if (client.isConnected()) {
      client.unsubscribe(topic).waitForCompletion(Math.max(1, timeout.toMillis()));
      cleanup.remove(topic);
    }
  }

  void remove(String topic, Duration timeout) throws Exception {
    await(
        submit(
            result -> {
              if (!desired.containsKey(topic)) return;
              try {
                client.unsubscribe(topic).waitForCompletion(Math.max(1, timeout.toMillis()));
                if (result.complete(null)) desired.remove(topic);
                else restoreBroker(timeout);
              } catch (Exception error) {
                // UNSUBACK 超时或调用方已放弃时保持原状态；在同一工作任务内恢复，避免越过后续变更。
                try {
                  if (client.isConnected()) restoreBroker(timeout);
                } catch (Exception restoreError) {
                  error.addSuppressed(restoreError);
                }
                throw error;
              }
            }),
        timeout.plusSeconds(1));
  }

  void unsubscribeOwned(Duration timeout) throws Exception {
    await(
        submit(
            result -> {
              var topics = new LinkedHashSet<>(desired.keySet());
              topics.addAll(cleanup);
              if (!topics.isEmpty() && client.isConnected()) {
                client
                    .unsubscribe(topics.toArray(String[]::new))
                    .waitForCompletion(Math.max(1, timeout.toMillis()));
              }
            }),
        timeout.plusSeconds(1));
  }

  private void subscribeBroker(String topic, int qos, long deadline) throws Exception {
    IMqttToken token =
        perTopicListener
            ? client.subscribe(
                topic,
                qos,
                (actualTopic, message) -> listener.onMessage(actualTopic, message.getPayload()))
            : client.subscribe(topic, qos);
    token.waitForCompletion(remaining(deadline));
    int[] granted = token.getGrantedQos();
    if (granted == null || granted.length != 1 || granted[0] < 0 || granted[0] > 2) {
      throw new IllegalStateException("Broker 拒绝 Topic 订阅: " + topic);
    }
  }

  private static long remaining(long deadline) throws java.util.concurrent.TimeoutException {
    long nanos = deadline - System.nanoTime();
    if (nanos <= 0) throw new java.util.concurrent.TimeoutException("订阅操作超时");
    return Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanos));
  }

  private CompletableFuture<Void> submit(Operation operation) {
    var result = new CompletableFuture<Void>();
    try {
      worker.execute(
          () -> {
            try {
              // 等待队列中已经超时或被中断的操作不得再修改 Broker。
              if (result.isDone()) return;
              operation.run(result);
              result.complete(null);
            } catch (Exception error) {
              result.completeExceptionally(error);
            }
          });
    } catch (java.util.concurrent.RejectedExecutionException error) {
      result.completeExceptionally(error);
    }
    return result;
  }

  static void await(CompletableFuture<Void> result, Duration timeout) throws Exception {
    try {
      result.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
    } catch (ExecutionException error) {
      if (error.getCause() instanceof Exception cause) throw cause;
      throw error;
    } catch (java.util.concurrent.TimeoutException | InterruptedException error) {
      if (error instanceof InterruptedException) Thread.currentThread().interrupt();
      if (result.completeExceptionally(error)) throw error;
      // ACK 与超时边界同时发生时，返回已经提交的结果，不能把已成功操作误报为失败。
      try {
        result.join();
      } catch (java.util.concurrent.CompletionException completed) {
        if (completed.getCause() instanceof Exception cause) throw cause;
        throw completed;
      }
    }
  }

  @Override
  public void close() {
    worker.shutdownNow();
  }

  @FunctionalInterface
  private interface Operation {
    void run(CompletableFuture<Void> result) throws Exception;
  }
}
