package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import org.junit.jupiter.api.Test;

class VdmSubscriptionTest {
  @Test
  void originalConfigConstructorKeepsWildcardSubscription() {
    var config = new VdmMqttClient.Config("localhost", 1883, VdmTopics.forDevice("TEST"),
        PayloadFormat.PROTOBUF, null, null, null, 1, Duration.ofSeconds(10));
    assertEquals(List.of("#"), config.subscriptionSuffixes());
  }

  @Test
  void explicitSubscriptionsAreImmutableAndValidated() {
    var suffixes = new ArrayList<>(List.of("telemetry", "attributes"));
    var config = config(suffixes);
    suffixes.add("rpc/resp");
    assertEquals(List.of("telemetry", "attributes"), config.subscriptionSuffixes());
    assertThrows(UnsupportedOperationException.class,
        () -> config.subscriptionSuffixes().add("rpc/resp"));
    assertThrows(IllegalArgumentException.class, () -> config(List.of()));
    assertThrows(IllegalArgumentException.class, () -> config(List.of("+/rpc/resp")));
  }

  @Test
  void rpcWithoutResponseSubscriptionFailsBeforePublishing() throws Exception {
    try (var client = new VdmMqttClient(config(List.of("telemetry", "attributes")), null, null)) {
      var error = assertThrows(IllegalStateException.class,
          () -> client.call("getAttr", Map.of(), Duration.ofSeconds(1)));
      assertTrue(error.getMessage().contains("rpc/resp"));
    }
  }

  @Test
  void independentClientsDoNotShareAFixedRequestSequence() throws Exception {
    var next = VdmMqttClient.class.getDeclaredMethod("nextReqId");
    next.setAccessible(true);
    var firstIds = new HashSet<Integer>();
    for (int i = 0; i < 16; i++) {
      try (var client = new VdmMqttClient(config(List.of("rpc/resp")), null, null)) {
        int id = (Integer) next.invoke(client);
        assertNotEquals(0, id);
        firstIds.add(id);
      }
    }
    // The old fixed sequence produced 1 for every independent connection.
    assertTrue(firstIds.size() > 1);
  }

  @Test
  void requestSequenceSkipsZeroAfterSignedInt32Wraparound() throws Exception {
    var next = VdmMqttClient.class.getDeclaredMethod("nextReqId");
    next.setAccessible(true);
    var field = VdmMqttClient.class.getDeclaredField("reqIds");
    field.setAccessible(true);
    try (var client = new VdmMqttClient(config(List.of("rpc/resp")), null, null)) {
      AtomicInteger counter = (AtomicInteger) field.get(client);
      counter.set(Integer.MAX_VALUE);
      assertEquals(Integer.MIN_VALUE, next.invoke(client));
      counter.set(-1);
      assertEquals(1, next.invoke(client));
    }
  }

  private static VdmMqttClient.Config config(List<String> suffixes) {
    return new VdmMqttClient.Config("localhost", 1883, VdmTopics.forDevice("TEST"),
        PayloadFormat.PROTOBUF, null, null, null, 1, Duration.ofSeconds(10), suffixes);
  }
}
