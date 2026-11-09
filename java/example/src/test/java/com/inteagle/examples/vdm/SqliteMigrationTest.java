package com.inteagle.examples.vdm;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** SDK 维护者回归测试：旧版三表无版本号数据库的无损迁移，以及失败后的原库保护。 */
class SqliteMigrationTest {
  @TempDir Path temporary;

  @Test
  void legacyRowsArePreservedAndDeliveryColumnsAreAdded() throws Exception {
    Path database = temporary.resolve("legacy.sqlite3");
    legacy(database, "TRIGGERED");
    try (var connection = SqliteDatabase.open(database);
        var sql = connection.createStatement()) {
      try (var rows =
          sql.executeQuery(
              "SELECT COUNT(*) FROM notification_outbox WHERE next_attempt_at IS NOT NULL")) {
        rows.next();
        assertEquals(1, rows.getInt(1));
      }
      try (var rows = sql.executeQuery("PRAGMA user_version")) {
        rows.next();
        assertEquals(1, rows.getInt(1));
      }
      try (var rows = sql.executeQuery("PRAGMA foreign_key_check")) {
        assertFalse(rows.next());
      }
    }
    // 再次启动只验证版本/校验和，不能重复迁移、丢行或重新创建通知。
    try (var store = new AlarmNotificationStore(database)) {
      assertEquals(1, store.pendingNotificationCount());
    }
  }

  @Test
  void invalidLegacyDataRollsBackEntireMigration() throws Exception {
    Path database = temporary.resolve("invalid.sqlite3");
    legacy(database, "INVALID");
    assertThrows(SQLException.class, () -> SqliteDatabase.open(database));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        var sql = connection.createStatement()) {
      try (var rows = sql.executeQuery("SELECT transition FROM alarm_events")) {
        assertTrue(rows.next());
        assertEquals("INVALID", rows.getString(1));
      }
      try (var rows = sql.executeQuery("PRAGMA user_version")) {
        rows.next();
        assertEquals(0, rows.getInt(1));
      }
    }
  }

  private void legacy(Path database, String transition) throws Exception {
    // 精确模拟最早示例：outbox 尚无租约/重试列，禁止直接使用新 schema 来伪造迁移输入。
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        var sql = connection.createStatement()) {
      sql.execute(
          "CREATE TABLE alarm_events(device_id TEXT,event_id TEXT,alarm_id TEXT,transition TEXT,received_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(device_id,event_id))");
      sql.execute(
          "CREATE TABLE alarm_incidents(device_id TEXT,alarm_id TEXT,active INTEGER,level TEXT,last_event_id TEXT,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(device_id,alarm_id))");
      sql.execute(
          "CREATE TABLE notification_outbox(device_id TEXT,event_id TEXT,notification TEXT,status TEXT DEFAULT 'PENDING',created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(device_id,event_id,notification))");
      try (var insert =
          connection.prepareStatement(
              "INSERT INTO alarm_events(device_id,event_id,alarm_id,transition) VALUES ('D','1','9',?)")) {
        insert.setString(1, transition);
        insert.executeUpdate();
      }
      sql.execute(
          "INSERT INTO alarm_incidents(device_id,alarm_id,active,level,last_event_id) VALUES ('D','9',1,'ALERT','1')");
      sql.execute(
          "INSERT INTO notification_outbox(device_id,event_id,notification) VALUES ('D','1','ALARM_TRIGGERED')");
    }
  }
}
