package com.inteagle.examples.vdm;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.List;
import org.sqlite.SQLiteConfig;

/** 示例专用 SQLite 初始化器：版本校验、独占迁移事务、校验和、旧示例数据迁移。 */
public final class SqliteDatabase {
  private SqliteDatabase() {}

  /** 打开数据库；创建目录、初始化或迁移失败时关闭连接，不留下部分 schema。 */
  public static Connection open(Path file) throws SQLException {
    try {
      Files.createDirectories(file.toAbsolutePath().getParent());
    } catch (Exception error) {
      throw new SQLException("无法创建 SQLite 数据目录", error);
    }
    // 在读取候选 outbox 前就获取写锁，避免多个 Worker 的读事务升级互相冲突。
    var config = new SQLiteConfig();
    config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
    Connection connection =
        DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath(), config.toProperties());
    try (Statement sql = connection.createStatement()) {
      sql.execute("PRAGMA busy_timeout = 5000");
      sql.execute("PRAGMA journal_mode = WAL");
      sql.execute("PRAGMA synchronous = FULL");
      sql.execute("PRAGMA foreign_keys = ON");
      migrate(connection);
      return connection;
    } catch (Exception error) {
      try {
        connection.close();
      } catch (SQLException closeError) {
        error.addSuppressed(closeError);
      }
      throw error instanceof SQLException databaseError
          ? databaseError
          : new SQLException("SQLite 初始化失败", error);
    }
  }

  private static void migrate(Connection connection) throws Exception {
    byte[] schema;
    try (var input = SqliteDatabase.class.getResourceAsStream("/db/sqlite/schema.sql")) {
      if (input == null) throw new SQLException("缺少 db/sqlite/schema.sql");
      schema = input.readAllBytes();
    }
    String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(schema));
    try (Statement sql = connection.createStatement()) {
      // 写锁先于版本检查获得，两个进程同时首次启动也不会重复执行迁移。
      sql.execute("BEGIN IMMEDIATE");
      try {
        int version;
        try (var rows = sql.executeQuery("PRAGMA user_version")) {
          rows.next();
          version = rows.getInt(1);
        }
        if (version > 1) throw new SQLException("数据库版本比当前示例新，拒绝降级打开: " + version);
        if (version == 0) {
          boolean legacy = tableExists(connection, "alarm_events");
          if (legacy) prepareLegacy(connection);
          // 只解析仓库内受控 DDL，不接收外部 SQL；脚本不包含触发器或字符串内分号。
          String ddl = new String(schema, StandardCharsets.UTF_8).replaceAll("(?m)^\\s*--.*$", "");
          for (String statement : ddl.split(";")) {
            if (!statement.isBlank()) sql.execute(statement);
          }
          if (legacy) copyLegacy(connection);
          try (var insert =
              connection.prepareStatement(
                  "INSERT INTO schema_history(version, description, checksum_sha256) VALUES (1, ?, ?)")) {
            insert.setString(1, "typed alarm inbox/outbox and displacement time series");
            insert.setString(2, checksum);
            insert.executeUpdate();
          }
          sql.execute("PRAGMA user_version = 1");
        } else {
          try (var rows =
              sql.executeQuery("SELECT checksum_sha256 FROM schema_history WHERE version = 1")) {
            if (!rows.next() || !checksum.equals(rows.getString(1))) {
              throw new SQLException("SQLite schema 校验和不匹配，请使用正式版本迁移");
            }
          }
        }
        try (var rows = sql.executeQuery("PRAGMA foreign_key_check")) {
          if (rows.next()) throw new SQLException("SQLite 外键完整性检查失败");
        }
        sql.execute("COMMIT");
      } catch (Exception error) {
        try {
          sql.execute("ROLLBACK");
        } catch (SQLException rollbackError) {
          error.addSuppressed(rollbackError);
        }
        throw error;
      }
    }
  }

  private static boolean tableExists(Connection connection, String table) throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")) {
      query.setString(1, table);
      try (var rows = query.executeQuery()) {
        return rows.next();
      }
    }
  }

  private static void prepareLegacy(Connection connection) throws SQLException {
    // 兼容旧仓库未标版本的三张表；部分表缺失或非法旧数据会回滚，不静默丢弃数据。
    try (Statement sql = connection.createStatement()) {
      for (String definition :
          List.of(
              "delivery_token TEXT",
              "locked_until TEXT",
              "next_attempt_at TEXT",
              "attempts INTEGER NOT NULL DEFAULT 0",
              "last_error TEXT",
              "sent_at TEXT")) {
        String name = definition.split(" ")[0];
        boolean exists = false;
        try (var rows = sql.executeQuery("PRAGMA table_info(notification_outbox)")) {
          while (rows.next()) exists |= rows.getString("name").equals(name);
        }
        if (!exists) sql.execute("ALTER TABLE notification_outbox ADD COLUMN " + definition);
      }
      for (String table : List.of("notification_outbox", "alarm_incidents", "alarm_events")) {
        sql.execute("ALTER TABLE " + table + " RENAME TO " + table + "_legacy");
      }
    }
  }

  private static void copyLegacy(Connection connection) throws SQLException {
    try (Statement sql = connection.createStatement()) {
      sql.executeUpdate("INSERT INTO alarm_events SELECT * FROM alarm_events_legacy");
      sql.executeUpdate("INSERT INTO alarm_incidents SELECT * FROM alarm_incidents_legacy");
      sql.executeUpdate(
          """
          INSERT INTO notification_outbox(device_id,event_id,notification,status,delivery_token,
            locked_until,next_attempt_at,attempts,last_error,sent_at,created_at)
          SELECT device_id,event_id,notification,status,
            CASE WHEN status='SENDING' THEN delivery_token END,
            CASE WHEN status='SENDING' THEN locked_until END,
            COALESCE(next_attempt_at,CURRENT_TIMESTAMP),attempts,substr(last_error,1,500),
            CASE WHEN status='SENT' THEN COALESCE(sent_at,created_at) END,created_at
          FROM notification_outbox_legacy
          """);
      for (String table : List.of("notification_outbox", "alarm_incidents", "alarm_events")) {
        sql.execute("DROP TABLE " + table + "_legacy");
      }
    }
  }
}
