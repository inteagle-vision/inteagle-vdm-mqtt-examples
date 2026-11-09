package com.inteagle.examples.vdm;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.VdmCodec;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import com.inteagle.vdm.mqtt.v1.DisplacementTelemetry;
import com.inteagle.vdm.mqtt.v1.TargetDisplacementSeries;
import com.inteagle.vdm.mqtt.v1.Telemetry;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** SDK 维护者回归测试：验证采样时间换算、格式一致性、重放幂等和事务回滚。 */
class DisplacementStoreTest {
  @TempDir Path temporary;

  @Test
  void protobufAndJsonStoreIdenticalSamplesAcrossRestart() throws Exception {
    var topics = VdmTopics.forDevice("D1");
    var telemetry =
        Telemetry.newBuilder()
            .setSchemaVersion(1)
            .setDisplacement(
                DisplacementTelemetry.newBuilder()
                    .setFirstSampleTimestampMs(10000)
                    .setSampleFrequencyHz(20)
                    .addTargets(
                        TargetDisplacementSeries.newBuilder().setTargetId("T1").addDx(1).addDx(2)))
            .build();
    var protobuf =
        new VdmCodec(PayloadFormat.PROTOBUF)
            .decode(topics.topic("telemetry"), topics, telemetry.toByteArray());
    var json =
        new VdmCodec(PayloadFormat.JSON)
            .decode(
                topics.topic("telemetry"),
                topics,
                "{\"disp\":{\"t\":10,\"f\":20,\"d\":{\"T1\":{\"dx\":[1,2]}}}}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var database = temporary.resolve("series.sqlite3");
    try (var store = new DisplacementStore(database)) {
      assertEquals(2, store.save("D1", protobuf));
      // 相同批次换成 JSON 重放，不能重复插入；另一设备则是独立序列。
      assertEquals(0, store.save("D1", json));
      assertEquals(2, store.save("D2", json));
    }
    try (var store = new DisplacementStore(database)) {
      var samples = store.query("D1", "T1", 10000, 10100, 10);
      assertEquals(2, samples.size());
      assertEquals(10050, samples.get(1).timeMs());
      assertEquals(2.0, samples.get(1).dx());
      assertNull(samples.get(1).dy()); // 没上报的轴必须保持 NULL，不能伪造为 0。
      assertNull(samples.get(1).dz());
      assertEquals(1, store.query("D1", "T1", 10000, 10050, 10).size());
      assertThrows(IllegalArgumentException.class, () -> store.query("D1", "T1", 0, 1, 10001));
    }
  }

  @Test
  void zOnlyZeroIsStoredAsMeasuredZeroWhileMissingAxesStayNull() throws Exception {
    // SDK 维护者回归测试：不能只根据 dx 的长度决定样本数；真实 dz=0 也不是缺失。
    var topics = VdmTopics.forDevice("Z_ONLY");
    var telemetry =
        Telemetry.newBuilder()
            .setSchemaVersion(1)
            .setDisplacement(
                DisplacementTelemetry.newBuilder()
                    .setFirstSampleTimestampMs(10000)
                    .setSampleFrequencyHz(1)
                    .addTargets(TargetDisplacementSeries.newBuilder().setTargetId("T1").addDz(0)))
            .build();
    for (PayloadFormat format : PayloadFormat.values()) {
      byte[] bytes =
          format == PayloadFormat.PROTOBUF
              ? telemetry.toByteArray()
              : "{\"disp\":{\"t\":10,\"f\":1,\"d\":{\"T1\":{\"dx\":[],\"dz\":[0]}}}}"
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      var message = new VdmCodec(format).decode(topics.topic("telemetry"), topics, bytes);
      try (var store = new DisplacementStore(temporary.resolve(format.name() + ".sqlite3"))) {
        assertEquals(1, store.save("Z_ONLY", message));
        var sample = store.query("Z_ONLY", "T1", 10000, 10001, 1).get(0);
        assertNull(sample.dx());
        assertNull(sample.dy());
        assertEquals(0.0, sample.dz());
      }
    }
  }

  @Test
  void badTargetRollsBackEarlierTargetsInSameBatch() throws Exception {
    var topics = VdmTopics.forDevice("D1");
    var data =
        new ObjectMapper()
            .readTree(
                """
        {"disp":{"t":10,"f":20,"d":{"valid":{"dx":[1]},"invalid":{"dx":[1,2],"dy":[1]}}}}
        """);
    var decoded =
        new VdmCodec(PayloadFormat.JSON)
            .decode(
                topics.topic("telemetry"),
                topics,
                data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    try (var store = new DisplacementStore(temporary.resolve("rollback.sqlite3"))) {
      assertThrows(IllegalArgumentException.class, () -> store.save("D1", decoded));
      assertTrue(store.query("D1", "valid", 0, 20000, 10).isEmpty());
    }
  }

  @Test
  void schemaEnforcesForeignKeysStatesAndVersion() throws Exception {
    var database = temporary.resolve("schema.sqlite3");
    try (var connection = SqliteDatabase.open(database);
        var sql = connection.createStatement()) {
      // SQL 约束是第二道边界，即使绕开 Java API 也不能插入悬空通知或非法状态。
      assertThrows(
          SQLException.class,
          () ->
              sql.executeUpdate(
                  "INSERT INTO notification_outbox(device_id,event_id,notification) VALUES ('D','1','ALARM_TRIGGERED')"));
      assertThrows(
          SQLException.class,
          () ->
              sql.executeUpdate(
                  "INSERT INTO alarm_events(device_id,event_id,alarm_id,transition) VALUES ('D','01','1','TRIGGERED')"));
      sql.execute("PRAGMA user_version = 2");
    }
    assertThrows(SQLException.class, () -> SqliteDatabase.open(database));
  }
}
