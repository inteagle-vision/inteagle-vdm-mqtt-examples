package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Objects;

/**
 * Durable alarm-event inbox, incident state and notification outbox.
 *
 * <p>The three writes happen in one SQLite transaction. A real notification worker should send
 * pending outbox rows after this transaction commits; it should never send from the MQTT callback
 * before persistence succeeds.
 */
public final class AlarmNotificationStore implements AutoCloseable {
  public enum Action {
    NOTIFY,
    STATE_ONLY,
    DUPLICATE
  }

  public record Decision(Action action, String notification, String reason) {}

  private record AlarmEvent(String eventId, String alarmId, String transition, String level) {}

  private record Incident(boolean exists, boolean active, String level) {}

  private static final BigInteger MAX_UNSIGNED_LONG =
      BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  private final Connection connection;

  public AlarmNotificationStore(Path database) throws SQLException {
    Objects.requireNonNull(database, "database");
    connection = DriverManager.getConnection("jdbc:sqlite:" + database.toAbsolutePath());
    configureDatabase();
    createSchema();
  }

  /** Process one decoded {@code vdm/{deviceId}/3A} payload. */
  public synchronized Decision process(String deviceId, JsonNode payload) throws SQLException {
    if (deviceId == null || deviceId.isBlank()) {
      throw new IllegalArgumentException("deviceId must not be blank");
    }
    AlarmEvent event = parseEvent(payload);

    boolean oldAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try {
      if (!insertEvent(deviceId, event)) {
        connection.rollback();
        return new Decision(
            Action.DUPLICATE, null, "deviceId + eventId was already processed");
      }

      Incident previous = findIncident(deviceId, event.alarmId());
      boolean notify = shouldNotify(event, previous);
      upsertIncident(deviceId, event);

      String notification = notify ? "ALARM_" + event.transition() : null;
      if (notification != null) {
        insertOutbox(deviceId, event.eventId(), notification);
      }
      connection.commit();
      return decision(event, notification);
    } catch (SQLException | RuntimeException error) {
      connection.rollback();
      throw error;
    } finally {
      connection.setAutoCommit(oldAutoCommit);
    }
  }

  public synchronized int pendingNotificationCount() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(
            "SELECT COUNT(*) FROM notification_outbox WHERE status = 'PENDING'")) {
      return rows.next() ? rows.getInt(1) : 0;
    }
  }

  private void configureDatabase() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA journal_mode = WAL");
      statement.execute("PRAGMA busy_timeout = 5000");
      statement.execute("PRAGMA foreign_keys = ON");
    }
  }

  private void createSchema() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("""
          CREATE TABLE IF NOT EXISTS alarm_events (
            device_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            alarm_id TEXT NOT NULL,
            transition TEXT NOT NULL,
            received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (device_id, event_id)
          )
          """);
      statement.execute("""
          CREATE TABLE IF NOT EXISTS alarm_incidents (
            device_id TEXT NOT NULL,
            alarm_id TEXT NOT NULL,
            active INTEGER NOT NULL,
            level TEXT,
            last_event_id TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (device_id, alarm_id)
          )
          """);
      statement.execute("""
          CREATE TABLE IF NOT EXISTS notification_outbox (
            device_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            notification TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (device_id, event_id, notification)
          )
          """);
    }
  }

  private boolean insertEvent(String deviceId, AlarmEvent event) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement("""
        INSERT OR IGNORE INTO alarm_events(device_id, event_id, alarm_id, transition)
        VALUES (?, ?, ?, ?)
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, event.eventId());
      statement.setString(3, event.alarmId());
      statement.setString(4, event.transition());
      return statement.executeUpdate() == 1;
    }
  }

  private Incident findIncident(String deviceId, String alarmId) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement("""
        SELECT active, level FROM alarm_incidents WHERE device_id = ? AND alarm_id = ?
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, alarmId);
      try (ResultSet row = statement.executeQuery()) {
        return row.next()
            ? new Incident(true, row.getInt("active") == 1, row.getString("level"))
            : new Incident(false, false, null);
      }
    }
  }

  private void upsertIncident(String deviceId, AlarmEvent event) throws SQLException {
    boolean active = !isTerminal(event.transition());
    try (PreparedStatement statement = connection.prepareStatement("""
        INSERT INTO alarm_incidents(
          device_id, alarm_id, active, level, last_event_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(device_id, alarm_id) DO UPDATE SET
          active = excluded.active,
          level = excluded.level,
          last_event_id = excluded.last_event_id,
          updated_at = CURRENT_TIMESTAMP
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, event.alarmId());
      statement.setInt(3, active ? 1 : 0);
      statement.setString(4, active ? event.level() : null);
      statement.setString(5, event.eventId());
      statement.executeUpdate();
    }
  }

  private void insertOutbox(String deviceId, String eventId, String notification)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement("""
        INSERT OR IGNORE INTO notification_outbox(device_id, event_id, notification)
        VALUES (?, ?, ?)
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, eventId);
      statement.setString(3, notification);
      statement.executeUpdate();
    }
  }

  private static boolean shouldNotify(AlarmEvent event, Incident previous) {
    return switch (event.transition()) {
      case "TRIGGERED" -> !previous.active();
      case "ESCALATED" -> !previous.active() || !Objects.equals(previous.level(), event.level());
      case "RECOVERED", "CANCELLED" -> previous.exists() && previous.active();
      case "DEESCALATED", "SYNCED" -> false;
      default -> throw new IllegalArgumentException("unsupported transition: " + event.transition());
    };
  }

  private static Decision decision(AlarmEvent event, String notification) {
    if (notification != null) {
      String reason = isTerminal(event.transition())
          ? "known active alarm reached a terminal state"
          : "new actionable " + event.transition() + " transition";
      return new Decision(Action.NOTIFY, notification, reason);
    }
    String reason = switch (event.transition()) {
      case "SYNCED" -> "connection recovery sync only updates current state";
      case "DEESCALATED" -> "default policy updates the lower level without notifying";
      case "RECOVERED", "CANCELLED" -> "terminal snapshot had no known active incident";
      case "TRIGGERED" -> "alarm was already active";
      case "ESCALATED" -> "alarm level did not change";
      default -> throw new IllegalArgumentException("unsupported transition: " + event.transition());
    };
    return new Decision(Action.STATE_ONLY, null, reason);
  }

  private static AlarmEvent parseEvent(JsonNode payload) {
    if (payload == null || !payload.isObject()) {
      throw new IllegalArgumentException("alarm payload must be a JSON object");
    }
    String eventId = unsignedId(payload, "eventId");
    String alarmId = unsignedId(payload, "alarmId");
    String transition = normalize(payload, "transition", "ALARM_TRANSITION_");
    boolean terminal = isTerminal(transition);
    JsonNode levelNode = payload.get("level");
    String level = levelNode == null || levelNode.isNull()
        ? null
        : stripPrefix(levelNode.asText(), "ALARM_LEVEL_");
    if (!terminal && (level == null || level.isBlank())) {
      throw new IllegalArgumentException(transition + " requires level");
    }
    if (terminal && level != null) {
      throw new IllegalArgumentException(transition + " must not contain level");
    }
    return new AlarmEvent(eventId, alarmId, transition, level);
  }

  private static String unsignedId(JsonNode payload, String field) {
    JsonNode node = payload.get(field);
    String value = node == null ? "" : node.asText();
    try {
      BigInteger parsed = new BigInteger(value);
      if (parsed.signum() <= 0 || parsed.compareTo(MAX_UNSIGNED_LONG) > 0) {
        throw new NumberFormatException();
      }
      return parsed.toString();
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(field + " must be a nonzero uint64 decimal string");
    }
  }

  private static String normalize(JsonNode payload, String field, String prefix) {
    JsonNode node = payload.get(field);
    String value = node == null ? "" : stripPrefix(node.asText(), prefix);
    if (value.isBlank()) {
      throw new IllegalArgumentException("alarm payload is missing " + field);
    }
    return value;
  }

  private static String stripPrefix(String value, String prefix) {
    String normalized = value.toUpperCase(Locale.ROOT);
    return normalized.startsWith(prefix) ? normalized.substring(prefix.length()) : normalized;
  }

  private static boolean isTerminal(String transition) {
    return transition.equals("RECOVERED") || transition.equals("CANCELLED");
  }

  @Override
  public synchronized void close() throws SQLException {
    connection.close();
  }
}
