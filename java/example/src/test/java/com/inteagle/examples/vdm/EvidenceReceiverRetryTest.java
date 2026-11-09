package com.inteagle.examples.vdm;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.vdm.mqtt.sdk.RpcException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;

/** SDK 维护者回归测试：注入模拟 ACK 和退避函数，验证重试策略；不连接设备、不实际等待退避。 */
class EvidenceReceiverRetryTest {
  // 限流与超时采用 1/2/4 秒有界退避；用记录函数代替真实睡眠。
  @Test
  void rateLimitAndTimeoutRetryWithBoundedBackoff() throws Exception {
    var attempts = new AtomicInteger();
    var delays = new ArrayList<Long>();
    String result =
        EvidenceReceiver.withAckRetry(
            () -> {
              int attempt = attempts.incrementAndGet();
              if (attempt <= 2) throw new RpcException(attempt, 4, "limited");
              if (attempt == 3) throw new TimeoutException("timeout");
              return "ACKED";
            },
            delays::add,
            ignored -> {});
    assertEquals("ACKED", result);
    assertEquals(4, attempts.get());
    assertEquals(List.of(1_000L, 2_000L, 4_000L), delays);
  }

  // 临时错误一直持续时最多尝试 4 次，避免无限循环占用接收线程。
  @Test
  void retryExhaustionStopsAfterFourAttempts() {
    var attempts = new AtomicInteger();
    var delays = new ArrayList<Long>();
    assertThrows(
        RpcException.class,
        () ->
            EvidenceReceiver.withAckRetry(
                () -> {
                  attempts.incrementAndGet();
                  throw new RpcException(10, 5, "timeout");
                },
                delays::add,
                ignored -> {}));
    assertEquals(4, attempts.get());
    assertEquals(3, delays.size());
  }

  // 参数等永久性错误应立即返回，重试不会改变结果。
  @Test
  void permanentErrorsNeverRetry() {
    for (Exception failure :
        List.of(
            new RpcException(1, 2, "bad parameters"),
            new RpcException(1, 3, "unsupported"),
            new MqttException(MqttException.REASON_CODE_NOT_AUTHORIZED))) {
      var attempts = new AtomicInteger();
      var delays = new ArrayList<Long>();
      assertThrows(
          Exception.class,
          () ->
              EvidenceReceiver.withAckRetry(
                  () -> {
                    attempts.incrementAndGet();
                    throw failure;
                  },
                  delays::add,
                  ignored -> {}));
      assertEquals(1, attempts.get());
      assertTrue(delays.isEmpty());
    }
  }

  // 异步 Future 包装的断线仍应识别为临时错误。
  @Test
  void temporaryDisconnectWrappedByFutureCanRetry() {
    assertTrue(
        EvidenceReceiver.isTemporaryAckFailure(
            new ExecutionException(
                new IllegalStateException(
                    "connection lost",
                    new MqttException(MqttException.REASON_CODE_CONNECTION_LOST)))));
    assertFalse(EvidenceReceiver.isTemporaryAckFailure(new IllegalArgumentException("bad hash")));
  }

  // 退避被中断时停止重试并保留中断语义，保证关闭过程不再发出请求。
  @Test
  void shutdownCancelsBackoffWithoutAnotherRequest() {
    var attempts = new AtomicInteger();
    try {
      assertThrows(
          InterruptedException.class,
          () ->
              EvidenceReceiver.withAckRetry(
                  () -> {
                    attempts.incrementAndGet();
                    throw new RpcException(1, 4, "limited");
                  },
                  ignored -> {
                    throw new InterruptedException("shutdown");
                  },
                  ignored -> {}));
      assertEquals(1, attempts.get());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
