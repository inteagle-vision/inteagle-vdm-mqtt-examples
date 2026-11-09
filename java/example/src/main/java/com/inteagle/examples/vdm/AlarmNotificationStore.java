package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * 告警事件 inbox、生命周期和通知 outbox 的 SQLite 持久化参考实现。
 *
 * <p>三类写入使用同一事务。后台 Worker 只能发送已提交的 outbox，不能先通知再落库。 建表、约束与索引见资源 db/sqlite/schema.sql；数据库不是
 * SDK/Starter 的必需依赖。
 */
public final class AlarmNotificationStore implements AutoCloseable {
  public enum Action {
    NOTIFY,
    STATE_ONLY,
    DUPLICATE
  }

  public record Decision(Action action, String notification, String reason) {}

  /** A leased outbox item. Only the holder of {@code deliveryToken} may complete or retry it. */
  public record Notification(
      String deviceId, String eventId, String alarmId, String notification, String deliveryToken) {
    public String idempotencyKey() {
      return deviceId + ":" + eventId + ":" + notification;
    }
  }

  private record AlarmEvent(String eventId, String alarmId, String transition, String level) {}

  private record Incident(boolean exists, boolean active, String level) {}

  private static final BigInteger MAX_UNSIGNED_LONG =
      BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  private final Connection connection;

  public AlarmNotificationStore(Path database) throws SQLException {
    Objects.requireNonNull(database, "database");
    connection = SqliteDatabase.open(database);
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
        return new Decision(Action.DUPLICATE, null, "deviceId + eventId was already processed");
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
        ResultSet rows =
            statement.executeQuery(
                "SELECT COUNT(*) FROM notification_outbox WHERE status = 'PENDING'")) {
      return rows.next() ? rows.getInt(1) : 0;
    }
  }

  private boolean insertEvent(String deviceId, AlarmEvent event) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
        INSERT INTO alarm_events(device_id, event_id, alarm_id, transition)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(device_id, event_id) DO NOTHING
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, event.eventId());
      statement.setString(3, event.alarmId());
      statement.setString(4, event.transition());
      return statement.executeUpdate() == 1;
    }
  }

  private Incident findIncident(String deviceId, String alarmId) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
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
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
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
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
        INSERT INTO notification_outbox(device_id, event_id, notification)
        VALUES (?, ?, ?)
        ON CONFLICT(device_id, event_id, notification) DO NOTHING
        """)) {
      statement.setString(1, deviceId);
      statement.setString(2, eventId);
      statement.setString(3, notification);
      statement.executeUpdate();
    }
  }

  /**
   * Atomically lease one ready notification for an external delivery worker.
   *
   * <p>A lease can be reclaimed after {@code leaseDuration}, which handles a worker crash. The
   * external delivery call must use {@link Notification#idempotencyKey()} because a crash after a
   * remote success but before {@link #markSent(Notification)} is necessarily at-least-once.
   */
  public synchronized Notification claimNext(String deliveryToken, Duration leaseDuration)
      throws SQLException {
    if (deliveryToken == null || deliveryToken.isBlank()) {
      throw new IllegalArgumentException("deliveryToken must not be blank");
    }
    if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    long leaseSeconds = Math.max(1, leaseDuration.toSeconds());
    boolean oldAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try {
      Notification candidate = findReadyNotification();
      if (candidate == null) {
        connection.commit();
        return null;
      }
      try (PreparedStatement statement =
          connection.prepareStatement(
              """
          UPDATE notification_outbox
          SET status = 'SENDING', delivery_token = ?,
              locked_until = datetime('now', ?), attempts = attempts + 1
          WHERE device_id = ? AND event_id = ? AND notification = ?
            AND (status = 'PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP
                 OR status = 'SENDING' AND locked_until <= CURRENT_TIMESTAMP)
          """)) {
        statement.setString(1, deliveryToken);
        statement.setString(2, "+" + leaseSeconds + " seconds");
        statement.setString(3, candidate.deviceId());
        statement.setString(4, candidate.eventId());
        statement.setString(5, candidate.notification());
        if (statement.executeUpdate() != 1) {
          connection.rollback();
          return null;
        }
      }
      connection.commit();
      return new Notification(
          candidate.deviceId(),
          candidate.eventId(),
          candidate.alarmId(),
          candidate.notification(),
          deliveryToken);
    } catch (SQLException | RuntimeException error) {
      connection.rollback();
      throw error;
    } finally {
      connection.setAutoCommit(oldAutoCommit);
    }
  }

  /** Mark a successfully delivered notification as terminal. */
  public synchronized void markSent(Notification notification) throws SQLException {
    updateLeasedNotification(
        notification,
        """
        UPDATE notification_outbox
        SET status = 'SENT', sent_at = CURRENT_TIMESTAMP, delivery_token = NULL, locked_until = NULL, last_error = NULL
        WHERE device_id = ? AND event_id = ? AND notification = ?
          AND status = 'SENDING' AND delivery_token = ?
        """);
  }

  /** Release a failed notification for a later retry. */
  public synchronized void retryLater(Notification notification, Duration delay, String error)
      throws SQLException {
    if (delay == null || delay.isNegative()) {
      throw new IllegalArgumentException("delay must not be negative");
    }
    long delaySeconds = Math.max(0, delay.toSeconds());
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
        UPDATE notification_outbox
        SET status = 'PENDING', delivery_token = NULL, locked_until = NULL,
            next_attempt_at = datetime('now', ?), last_error = ?
        WHERE device_id = ? AND event_id = ? AND notification = ?
          AND status = 'SENDING' AND delivery_token = ?
        """)) {
      statement.setString(1, "+" + delaySeconds + " seconds");
      statement.setString(
          2, error == null ? "delivery failed" : error.substring(0, Math.min(500, error.length())));
      statement.setString(3, notification.deviceId());
      statement.setString(4, notification.eventId());
      statement.setString(5, notification.notification());
      statement.setString(6, notification.deliveryToken());
      if (statement.executeUpdate() != 1) {
        throw new IllegalStateException("notification lease was lost before retry");
      }
    }
  }

  private Notification findReadyNotification() throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            """
        SELECT outbox.device_id, outbox.event_id, events.alarm_id, outbox.notification
        FROM notification_outbox AS outbox
        JOIN alarm_events AS events
          ON events.device_id = outbox.device_id AND events.event_id = outbox.event_id
        WHERE (outbox.status = 'PENDING'
               AND outbox.next_attempt_at <= CURRENT_TIMESTAMP)
           OR (outbox.status = 'SENDING' AND outbox.locked_until <= CURRENT_TIMESTAMP)
        ORDER BY outbox.created_at, outbox.event_id
        LIMIT 1
        """)) {
      try (ResultSet row = statement.executeQuery()) {
        return row.next()
            ? new Notification(
                row.getString("device_id"),
                row.getString("event_id"),
                row.getString("alarm_id"),
                row.getString("notification"),
                null)
            : null;
      }
    }
  }

  private void updateLeasedNotification(Notification notification, String sql) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, notification.deviceId());
      statement.setString(2, notification.eventId());
      statement.setString(3, notification.notification());
      statement.setString(4, notification.deliveryToken());
      if (statement.executeUpdate() != 1) {
        throw new IllegalStateException("notification lease was lost before completion");
      }
    }
  }

  private static boolean shouldNotify(AlarmEvent event, Incident previous) {
    return switch (event.transition()) {
      case "TRIGGERED" -> !previous.active();
      case "ESCALATED" -> !previous.active() || !Objects.equals(previous.level(), event.level());
      case "RECOVERED", "CANCELLED" -> previous.exists() && previous.active();
      case "DEESCALATED", "SYNCED" -> false;
      default ->
          throw new IllegalArgumentException("unsupported transition: " + event.transition());
    };
  }

  private static Decision decision(AlarmEvent event, String notification) {
    if (notification != null) {
      String reason =
          isTerminal(event.transition())
              ? "known active alarm reached a terminal state"
              : "new actionable " + event.transition() + " transition";
      return new Decision(Action.NOTIFY, notification, reason);
    }
    String reason =
        switch (event.transition()) {
          case "SYNCED" -> "connection recovery sync only updates current state";
          case "DEESCALATED" -> "default policy updates the lower level without notifying";
          case "RECOVERED", "CANCELLED" -> "terminal snapshot had no known active incident";
          case "TRIGGERED" -> "alarm was already active";
          case "ESCALATED" -> "alarm level did not change";
          default ->
              throw new IllegalArgumentException("unsupported transition: " + event.transition());
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
    String level =
        levelNode == null || levelNode.isNull()
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
