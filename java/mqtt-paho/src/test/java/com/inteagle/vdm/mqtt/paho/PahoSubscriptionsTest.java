package com.inteagle.vdm.mqtt.paho;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** SDK 维护者回归测试：模拟重连阻塞和迟到 ACK，超时的订阅变更不能在后台意外生效。 */
class PahoSubscriptionsTest {
  private static final String A = "vdm/A/#";
  private static final String B = "vdm/B/#";
  private static final Duration TIMEOUT = Duration.ofMillis(10);

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void queuedMutationIsDiscardedAfterCallerTimeout(boolean adding) throws Exception {
    var client = client();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var blocked = blockedToken(entered, release);
    var accepted = token();
    when(client.subscribe(A, 1)).thenReturn(blocked, accepted);
    try (var subscriptions = subscriptions(client)) {
      var restoring = subscriptions.restore(Duration.ofSeconds(5));
      try {
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> mutate(subscriptions, adding))
            .isInstanceOf(TimeoutException.class);
      } finally {
        release.countDown();
      }
      restoring.get(2, TimeUnit.SECONDS);
      subscriptions.restore(Duration.ofSeconds(2)).get(2, TimeUnit.SECONDS);
      verify(client, never()).subscribe(B, 1);
      verify(client, never()).unsubscribe(anyString());
      verify(client, times(2)).subscribe(A, 1); // 原有订阅仍被恢复。
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void lateAckRollsBackInsteadOfCommittingAfterTimeout(boolean adding) throws Exception {
    var client = client();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var blocked = blockedToken(entered, release);
    if (adding) when(client.subscribe(B, 1)).thenReturn(blocked);
    else when(client.unsubscribe(A)).thenReturn(blocked);
    try (var subscriptions = subscriptions(client)) {
      try {
        assertThatThrownBy(() -> mutate(subscriptions, adding))
            .isInstanceOf(TimeoutException.class);
        assertThat(entered.getCount()).isZero();
      } finally {
        release.countDown();
      }
      // 此屏障排在迟到 ACK 及其回滚之后，并模拟后续重连。
      subscriptions.restore(Duration.ofSeconds(2)).get(2, TimeUnit.SECONDS);
      verify(client, atLeastOnce()).subscribe(A, 1);
      if (adding) {
        verify(client, times(1)).subscribe(B, 1); // 重连不得再订阅调用方已认为失败的 B。
        verify(client).unsubscribe(B);
      } else {
        verify(client).unsubscribe(A);
        verify(client, times(2)).subscribe(A, 1); // 回滚一次、后续恢复一次。
      }
    }
  }

  private void mutate(PahoSubscriptions subscriptions, boolean adding) throws Exception {
    if (adding) subscriptions.add(B, 1, TIMEOUT);
    else subscriptions.remove(A, TIMEOUT);
  }

  private IMqttAsyncClient client() throws Exception {
    var client = mock(IMqttAsyncClient.class);
    var accepted = token();
    when(client.isConnected()).thenReturn(true);
    when(client.subscribe(anyString(), anyInt())).thenReturn(accepted);
    when(client.unsubscribe(anyString())).thenReturn(accepted);
    return client;
  }

  private IMqttToken token() {
    var token = mock(IMqttToken.class);
    when(token.getGrantedQos()).thenReturn(new int[] {1});
    return token;
  }

  private IMqttToken blockedToken(CountDownLatch entered, CountDownLatch release) throws Exception {
    var token = token();
    doAnswer(
            invocation -> {
              entered.countDown();
              if (!release.await(5, TimeUnit.SECONDS)) throw new TimeoutException("测试未释放 ACK");
              return null;
            })
        .when(token)
        .waitForCompletion(anyLong());
    return token;
  }

  private PahoSubscriptions subscriptions(IMqttAsyncClient client) {
    return new PahoSubscriptions(
        client,
        new VdmMqttTransport.Listener() {
          public void onMessage(String topic, byte[] payload) {}

          public void onDisconnected(Throwable error) {}

          public void onError(Throwable error) {}
        },
        false,
        List.of(A),
        1);
  }
}
