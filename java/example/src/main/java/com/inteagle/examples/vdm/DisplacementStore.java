package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.JsonNode;
import com.inteagle.vdm.mqtt.sdk.DecodedPayload;
import com.inteagle.vdm.mqtt.v1.Telemetry;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * 可选 SQLite 位移时序示例。每个采样点单独保存，可按设备、标靶和时间范围查询。
 *
 * <p>支持 JSON/Protobuf，整批事务提交，重复投递幂等；缺失方向保存 NULL。 适用于本地接入演示/小规模采集，不等同于面向大规模设备的分布式时序数据库。
 */
public final class DisplacementStore implements AutoCloseable {
  /** 采样时间为 UTC Unix 毫秒；dx/dy/dz 单位 mm，null 表示该方向未上报。 */
  public record Sample(
      String deviceId, String targetId, long timeMs, Double dx, Double dy, Double dz) {}

  private final Connection connection;

  public DisplacementStore(Path database) throws SQLException {
    connection = SqliteDatabase.open(database);
  }

  /** 保存位移批次并返回新插入点数；其他遥测返回 0，非法批次整体回滚。 */
  public synchronized int save(String deviceId, DecodedPayload message) throws SQLException {
    if (!message.suffix().equals("telemetry")) return 0;
    long start;
    int frequency;
    var targets = new ArrayList<Target>();
    if (message.value() instanceof Telemetry telemetry) {
      if (!telemetry.hasDisplacement()) return 0;
      var displacement = telemetry.getDisplacement();
      start = displacement.getFirstSampleTimestampMs();
      frequency = displacement.getSampleFrequencyHz();
      for (var target : displacement.getTargetsList()) {
        targets.add(
            new Target(
                target.getTargetId(), target.getDxList(), target.getDyList(), target.getDzList()));
      }
    } else {
      JsonNode displacement = message.data().path("disp");
      if (displacement.isMissingNode()) return 0;
      // MQTT JSON 的 t 是 Unix 秒；Protobuf 已直接提供 firstSampleTimestampMs。
      try {
        start = new BigDecimal(displacement.path("t").asText()).movePointRight(3).longValueExact();
        frequency = new BigDecimal(displacement.path("f").asText()).intValueExact();
      } catch (RuntimeException error) {
        throw new IllegalArgumentException("位移 t/f 非法", error);
      }
      var data = displacement.path("d");
      if (!data.isObject()) throw new IllegalArgumentException("disp.d 必须是标靶对象");
      data.properties()
          .iterator()
          .forEachRemaining(
              entry ->
                  targets.add(
                      new Target(
                          entry.getKey(),
                          axis(entry.getValue(), "dx"),
                          axis(entry.getValue(), "dy"),
                          axis(entry.getValue(), "dz"))));
    }
    if (start < 0 || frequency < 1 || frequency > 60) {
      throw new IllegalArgumentException("位移时间或采样频率非法");
    }
    connection.setAutoCommit(false);
    try (var insert =
        connection.prepareStatement(
            """
        INSERT INTO displacement_samples(device_id,target_id,sample_time_ms,batch_start_ms,
          sample_index,sample_frequency_hz,dx_mm,dy_mm,dz_mm) VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(device_id,target_id,sample_time_ms) DO NOTHING
        """)) {
      int added = 0;
      for (Target target : targets) {
        int size = Math.max(target.dx.size(), Math.max(target.dy.size(), target.dz.size()));
        for (List<? extends Number> axis : List.of(target.dx, target.dy, target.dz)) {
          if (!axis.isEmpty() && axis.size() != size)
            throw new IllegalArgumentException("位移各非空轴样本数必须一致");
        }
        for (int i = 0; i < size; i++) {
          insert.setString(1, deviceId);
          insert.setString(2, target.id);
          insert.setLong(3, Math.addExact(start, Math.round(i * 1000.0 / frequency)));
          insert.setLong(4, start);
          insert.setInt(5, i);
          insert.setInt(6, frequency);
          setAxis(insert, 7, target.dx, i);
          setAxis(insert, 8, target.dy, i);
          setAxis(insert, 9, target.dz, i);
          added += insert.executeUpdate();
        }
      }
      connection.commit();
      return added;
    } catch (SQLException | RuntimeException error) {
      connection.rollback();
      throw error;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  /** 查询 [fromMs, toMs) 时间范围，最多返回 10000 点，防止一次加载整个历史库。 */
  public synchronized List<Sample> query(
      String deviceId, String targetId, long fromMs, long toMs, int limit) throws SQLException {
    if (fromMs < 0 || toMs <= fromMs || limit < 1 || limit > 10000)
      throw new IllegalArgumentException("查询范围或 limit 非法");
    try (var query =
        connection.prepareStatement(
            """
        SELECT sample_time_ms, dx_mm, dy_mm, dz_mm FROM displacement_samples
        WHERE device_id=? AND target_id=? AND sample_time_ms>=? AND sample_time_ms<?
        ORDER BY sample_time_ms LIMIT ?
        """)) {
      query.setString(1, deviceId);
      query.setString(2, targetId);
      query.setLong(3, fromMs);
      query.setLong(4, toMs);
      query.setInt(5, limit);
      var result = new ArrayList<Sample>();
      try (var rows = query.executeQuery()) {
        while (rows.next())
          result.add(
              new Sample(
                  deviceId,
                  targetId,
                  rows.getLong(1),
                  (Double) rows.getObject(2),
                  (Double) rows.getObject(3),
                  (Double) rows.getObject(4)));
      }
      return List.copyOf(result);
    }
  }

  private record Target(
      String id, List<? extends Number> dx, List<? extends Number> dy, List<? extends Number> dz) {}

  private static List<Double> axis(JsonNode node, String field) {
    JsonNode values = node.get(field);
    if (values == null) return List.of();
    if (!values.isArray()) throw new IllegalArgumentException(field + " 必须是数组");
    var result = new ArrayList<Double>();
    for (JsonNode value : values) {
      if (!value.isNumber()) throw new IllegalArgumentException(field + " 包含非数值样本");
      result.add(value.doubleValue());
    }
    return result;
  }

  private static void setAxis(
      java.sql.PreparedStatement sql, int parameter, List<? extends Number> values, int index)
      throws SQLException {
    if (values.isEmpty()) sql.setNull(parameter, Types.REAL);
    else {
      double value = values.get(index).doubleValue();
      if (!Double.isFinite(value)) throw new IllegalArgumentException("位移必须是有限数值");
      sql.setDouble(parameter, value);
    }
  }

  @Override
  public synchronized void close() throws SQLException {
    connection.close();
  }
}
