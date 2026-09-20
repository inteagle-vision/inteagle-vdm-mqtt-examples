package com.inteagle.vdm.service;

import com.fasterxml.jackson.databind.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** One process, serialized SQLite transactions, WAL and FULL sync before MQTT ACK. */
public final class Store implements AutoCloseable {
  public static final class CapacityException extends RuntimeException {
    public CapacityException() {
      super("durable inbox capacity exhausted");
    }
  }

  public static final ObjectMapper JSON = new ObjectMapper();
  private final Connection db;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final int maxInbox;
  private final long maxAge;
  private final long maxEvidenceBytes;
  private final Path evidenceRoot;

  public Store(Path dir, int maxInbox, long maxAge) throws Exception {
    this(dir, maxInbox, maxAge, 256L * 1024 * 1024);
  }

  public Store(Path dir, int maxInbox, long maxAge, long maxEvidenceBytes) throws Exception {
    Files.createDirectories(dir);
    this.maxEvidenceBytes = maxEvidenceBytes;
    this.evidenceRoot = dir.resolve("evidence");
    this.maxInbox = maxInbox;
    this.maxAge = maxAge;
    lockChannel =
        FileChannel.open(
            dir.resolve("service.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    FileLock acquired;
    try {
      acquired = lockChannel.tryLock();
      if (acquired == null) throw new IllegalStateException("data directory already in use");
    } catch (Exception e) {
      lockChannel.close();
      throw e;
    }
    lock = acquired;
    try {
      db =
          DriverManager.getConnection(
              "jdbc:sqlite:" + dir.resolve("service.sqlite").toAbsolutePath());
      rows("PRAGMA journal_mode=WAL");
      update("PRAGMA synchronous=FULL");
      rows("PRAGMA busy_timeout=5000");
      int version =
          ((Number) rows("PRAGMA user_version").getFirst().get("user_version")).intValue();
      if (version > 1) throw new IllegalStateException("unsupported database schema " + version);
      transaction(
          () -> {
            update(
                "CREATE TABLE IF NOT EXISTS inbox(id INTEGER PRIMARY KEY AUTOINCREMENT,c TEXT NOT"
                    + " NULL,d TEXT NOT NULL,topic TEXT NOT NULL,payload BLOB NOT NULL,status TEXT"
                    + " NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_at"
                    + " INTEGER NOT NULL DEFAULT 0,error TEXT,created INTEGER NOT NULL)");
            update(
                "CREATE TABLE IF NOT EXISTS latest(c TEXT,d TEXT,telemetry TEXT,attributes"
                    + " TEXT,count INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(c,d))");
            update(
                "CREATE TABLE IF NOT EXISTS alarm_events(c TEXT,d TEXT,event_id TEXT,alarm_id"
                    + " TEXT,ts INTEGER,payload TEXT,created INTEGER,PRIMARY KEY(c,d,event_id))");
            update(
                "CREATE TABLE IF NOT EXISTS incidents(c TEXT,d TEXT,alarm_id TEXT,event_id TEXT,ts"
                    + " INTEGER,transition TEXT,payload TEXT,needs_reconcile INTEGER DEFAULT"
                    + " 0,reconcile_version INTEGER DEFAULT 0,current_active INTEGER,current_state"
                    + " TEXT,reconciled_at INTEGER,PRIMARY KEY(c,d,alarm_id))");
            update(
                "CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY,c TEXT,d TEXT,payload"
                    + " TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next_at"
                    + " INTEGER DEFAULT 0,error TEXT,created INTEGER)");
            update(
                "CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,c TEXT,d TEXT,kind"
                    + " TEXT,payload TEXT,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT"
                    + " 0,next_at INTEGER DEFAULT 0,error TEXT,created INTEGER)");
            update(
                "CREATE TABLE IF NOT EXISTS evidence_events(id TEXT PRIMARY KEY,c TEXT,d"
                    + " TEXT,payload TEXT,created INTEGER)");
            update("CREATE INDEX IF NOT EXISTS inbox_ready ON inbox(status,next_at,id)");
            if (rows("PRAGMA table_info(incidents)").stream()
                .noneMatch(r -> r.get("name").equals("reconcile_version")))
              update("ALTER TABLE incidents ADD COLUMN reconcile_version INTEGER DEFAULT 0");
            for (String definition :
                List.of("current_active INTEGER", "current_state TEXT", "reconciled_at INTEGER")) {
              String column = definition.split(" ")[0];
              if (rows("PRAGMA table_info(incidents)").stream()
                  .noneMatch(r -> r.get("name").equals(column)))
                update("ALTER TABLE incidents ADD COLUMN " + definition);
            }
            update(
                "CREATE TABLE IF NOT EXISTS evidence_receipts(c TEXT,d TEXT,event_id TEXT,hash"
                    + " TEXT,path TEXT,created INTEGER,deleted INTEGER DEFAULT 0,PRIMARY"
                    + " KEY(c,d,event_id,hash))");
            update(
                "CREATE TABLE IF NOT EXISTS evidence_chunks(inbox_id INTEGER PRIMARY KEY,"
                    + " package_key TEXT)");
            update("update inbox set status='pending' where status='assembling'");
            update("PRAGMA user_version=1");
          });
    } catch (Exception e) {
      lock.release();
      lockChannel.close();
      throw e;
    }
  }

  @FunctionalInterface
  public interface Work {
    void run() throws Exception;
  }

  public synchronized void transaction(Work work) throws Exception {
    db.setAutoCommit(false);
    try {
      work.run();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    } finally {
      db.setAutoCommit(true);
    }
  }

  public synchronized int update(String sql, Object... args) throws SQLException {
    try (var p = db.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
      return p.executeUpdate();
    }
  }

  public synchronized List<Map<String, Object>> rows(String sql, Object... args)
      throws SQLException {
    try (var p = db.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
      try (var r = p.executeQuery()) {
        var result = new ArrayList<Map<String, Object>>();
        var md = r.getMetaData();
        while (r.next()) {
          var row = new LinkedHashMap<String, Object>();
          for (int i = 1; i <= md.getColumnCount(); i++)
            row.put(md.getColumnLabel(i), r.getObject(i));
          result.add(row);
        }
        return result;
      }
    }
  }

  public synchronized void enqueue(String c, String d, String topic, byte[] payload)
      throws Exception {
    transaction(
        () -> {
          int total =
              ((Number) rows("select count(*) n from inbox").getFirst().get("n")).intValue();
          if (total >= maxInbox)
            update(
                "delete from inbox where id in(select id from inbox where status='done' order by id"
                    + " limit ?)",
                total - maxInbox + 1);
          if (((Number) rows("select count(*) n from inbox").getFirst().get("n")).intValue()
              >= maxInbox) throw new CapacityException();
          boolean image = topic.endsWith("/image");
          long limit = image ? Math.min(32L * 1024 * 1024, maxEvidenceBytes) : 1024 * 1024;
          if (payload.length > limit)
            throw new IllegalArgumentException("MQTT payload exceeds topic limit");
          if (image
              && pendingEvidenceBytes(-1) + evidenceReservedBytes() + payload.length
                  > maxEvidenceBytes) throw new CapacityException();
          update(
              "insert into inbox(c,d,topic,payload,created) values(?,?,?,?,?)",
              c,
              d,
              topic,
              payload,
              now());
        });
  }

  /** Pending raw image bytes are reserved until processing has durably finished. */
  public synchronized long pendingEvidenceBytes(long excludingInboxId) throws Exception {
    return ((Number)
            rows(
                    "select coalesce(sum(length(payload)),0) n from inbox where status!='done' and"
                        + " substr(topic,-6)='/image' and id!=?",
                    excludingInboxId)
                .getFirst()
                .get("n"))
        .longValue();
  }

  public synchronized long evidenceReservedBytes() throws Exception {
    if (!Files.exists(evidenceRoot)) return 0;
    long used = 0;
    try (var paths = Files.walk(evidenceRoot)) {
      for (var p :
          paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).toList())
        used += Files.size(p);
    }
    // Reserve unfilled package space during ingress too, so later JPEGs cannot consume it.
    for (var row : rows("select distinct package_key from evidence_chunks")) {
      String key = (String) row.get("package_key");
      var chunks =
          rows(
              "select payload from inbox where id in(select inbox_id from evidence_chunks where"
                  + " package_key=?) limit 1",
              key);
      Path partial = evidenceRoot.resolve(key + ".tar.part");
      if (!chunks.isEmpty() && Files.isRegularFile(partial, LinkOption.NOFOLLOW_LINKS)) {
        byte[] raw = (byte[]) chunks.getFirst().get("payload");
        long length = java.nio.ByteBuffer.wrap(raw, 12, 8).getLong();
        used += Math.max(0, length - Files.size(partial)) + 4096;
      }
    }
    return used;
  }

  public synchronized void latest(String c, String d, String suffix, JsonNode payload)
      throws Exception {
    if (!Set.of("telemetry", "attributes").contains(suffix))
      throw new IllegalArgumentException("invalid latest suffix");
    update(
        "insert into latest(c,d,"
            + suffix
            + ",count) values(?,?,?,1) on conflict(c,d) do update set "
            + suffix
            + "=excluded."
            + suffix
            + ",count=count+1",
        c,
        d,
        payload.toString());
  }

  public synchronized Map<String, Object> latest(String c, String d) throws Exception {
    var rows = rows("select * from latest where c=? and d=?", c, d);
    var out = new LinkedHashMap<String, Object>();
    out.put("telemetry", null);
    out.put("attributes", null);
    out.put("receivedCount", 0);
    if (!rows.isEmpty()) {
      var r = rows.getFirst();
      for (var k : List.of("telemetry", "attributes"))
        if (r.get(k) != null) out.put(k, JSON.readTree((String) r.get(k)));
      out.put("receivedCount", ((Number) r.get("count")).intValue());
    }
    return out;
  }

  public synchronized void event(String c, String d, JsonNode p) throws Exception {
    transaction(
        () -> {
          update(
              "insert or ignore into evidence_events values(?,?,?,?,?)",
              hash(JSON.writeValueAsString(List.of(c, d, p))),
              c,
              d,
              p.toString(),
              now());
          trimHistory("evidence_events");
        });
  }

  public synchronized void alarm(String c, String d, JsonNode payload) throws Exception {
    var p = payload.deepCopy();
    String event = id(p, "eventId"), alarm = id(p, "alarmId");
    String transition =
        p.path("transition").asText().replace("ALARM_TRANSITION_", "").toUpperCase(Locale.ROOT);
    if (!Set.of("TRIGGERED", "ESCALATED", "DEESCALATED", "RECOVERED", "CANCELLED", "SYNCED")
        .contains(transition)) throw new IllegalArgumentException("unknown alarm transition");
    long ts = Long.parseLong(p.path("ts").asText());
    if (ts <= 0) throw new IllegalArgumentException("missing alarm ts");
    ((com.fasterxml.jackson.databind.node.ObjectNode) p).put("transition", transition);
    if (p.has("level"))
      ((com.fasterxml.jackson.databind.node.ObjectNode) p)
          .put("level", p.get("level").asText().replace("ALARM_LEVEL_", ""));
    transaction(
        () -> {
          if (update(
                  "insert or ignore into alarm_events values(?,?,?,?,?,?,?)",
                  c,
                  d,
                  event,
                  alarm,
                  ts,
                  p.toString(),
                  now())
              == 0) return;
          trimHistory("alarm_events");
          var old = rows("select * from incidents where c=? and d=? and alarm_id=?", c, d, alarm);
          if (!old.isEmpty() && ts <= ((Number) old.getFirst().get("ts")).longValue()) {
            update(
                "update incidents set needs_reconcile=1,reconcile_version=reconcile_version+1 where"
                    + " c=? and d=? and alarm_id=?",
                c,
                d,
                alarm);
            schedule(c, d, "reconcile", Map.of(), "reconcile:" + c + ":" + d, true);
            return;
          }
          update(
              "insert into incidents(c,d,alarm_id,event_id,ts,transition,payload)"
                  + " values(?,?,?,?,?,?,?) on conflict(c,d,alarm_id) do update set"
                  + " event_id=excluded.event_id,ts=excluded.ts,transition=excluded.transition,payload=excluded.payload,current_active=null,current_state=null,reconciled_at=null",
              c,
              d,
              alarm,
              event,
              ts,
              transition,
              p.toString());
          String previousLevel =
              old.isEmpty()
                  ? null
                  : JSON.readTree((String) old.getFirst().get("payload")).path("level").asText();
          boolean eligible =
              switch (transition) {
                case "TRIGGERED" -> old.isEmpty();
                case "ESCALATED" ->
                    old.isEmpty() || levelRank(p.path("level").asText()) > levelRank(previousLevel);
                case "RECOVERED", "CANCELLED" -> true;
                default -> false;
              };
          if (eligible && ts <= now() && now() - ts <= maxAge) {
            String key = hash(JSON.writeValueAsString(List.of(c, d, event, transition)));
            if (rows("select 1 from outbox where id=?", key).isEmpty()
                && ((Number)
                            rows("select count(*) n from outbox where status!='done'")
                                .getFirst()
                                .get("n"))
                        .longValue()
                    >= maxInbox)
              throw new IllegalStateException("notification outbox capacity exhausted");
            update(
                "insert or ignore into outbox(id,c,d,payload,created) values(?,?,?,?,?)",
                key,
                c,
                d,
                JSON.writeValueAsString(
                    Map.of("connectionId", c, "deviceId", d, "event", p, "transition", transition)),
                now());
          }
        });
  }

  private static int levelRank(String value) {
    return value == null
        ? 0
        : switch (value) {
          case "ALERT" -> 1;
          case "ALARM" -> 2;
          case "ACTION" -> 3;
          default -> 0;
        };
  }

  private void trimHistory(String table) throws Exception {
    if (!Set.of("alarm_events", "evidence_events").contains(table))
      throw new IllegalArgumentException();
    update(
        "delete from "
            + table
            + " where rowid in(select rowid from "
            + table
            + " order by created,rowid limit max(0,(select count(*) from "
            + table
            + ")-?))",
        10L * maxInbox);
  }

  private static String id(JsonNode p, String field) {
    try {
      var n = new java.math.BigInteger(p.path(field).asText());
      if (n.signum() <= 0 || n.bitLength() > 64) throw new NumberFormatException();
      return n.toString();
    } catch (Exception e) {
      throw new IllegalArgumentException(field + " must be uint64");
    }
  }

  public synchronized void schedule(
      String c, String d, String kind, Object payload, String id, boolean rearm) throws Exception {
    update(
        "insert into jobs(id,c,d,kind,payload,created) values(?,?,?,?,?,?) on conflict(id) do "
            + (rearm ? "update set status='pending',attempts=0,next_at=0" : "nothing"),
        id,
        c,
        d,
        kind,
        JSON.writeValueAsString(payload),
        now());
  }

  public synchronized void retry(String table, Object id, int attempts, Exception error)
      throws Exception {
    if (!Set.of("inbox", "jobs", "outbox").contains(table)) throw new IllegalArgumentException();
    update(
        "update " + table + " set attempts=?,status=?,next_at=?,error=? where id=?",
        attempts,
        attempts >= 8 ? "failed" : "pending",
        now() + Math.min(300, 1L << Math.min(attempts, 8)),
        error.getClass().getSimpleName(),
        id);
  }

  public synchronized Map<String, Object> counters() throws Exception {
    var c = new LinkedHashMap<String, Object>();
    for (String table : List.of("inbox", "outbox", "jobs"))
      for (String status : List.of("pending", "failed"))
        c.put(
            table + "_" + status,
            rows("select count(*) n from " + table + " where status=?", status)
                .getFirst()
                .get("n"));
    return c;
  }

  public synchronized boolean verified(String c, String d, String event, String hash)
      throws Exception {
    var found =
        rows(
            "select path from evidence_receipts where c=? and d=? and event_id=? and hash=? and"
                + " deleted=0",
            c,
            d,
            event,
            hash);
    return !found.isEmpty()
        && Files.isRegularFile(
            Path.of((String) found.getFirst().get("path")), LinkOption.NOFOLLOW_LINKS);
  }

  public synchronized void prune(int days) throws Exception {
    long before = now() - days * 86400L;
    for (String t : List.of("inbox", "outbox", "jobs"))
      update("delete from " + t + " where status='done' and created<?", before);
    update(
        "delete from alarm_events where created<? and not exists(select 1 from outbox o where"
            + " o.c=alarm_events.c and o.d=alarm_events.d and o.status='pending')",
        before);
    update("delete from evidence_events where created<?", before);
    trimHistory("alarm_events");
    trimHistory("evidence_events");
    update(
        "delete from incidents where coalesce(reconciled_at,ts)<? and needs_reconcile=0 and"
            + " (current_active=0 or (current_active is null and transition"
            + " in('RECOVERED','CANCELLED')))",
        before);
  }

  public static long now() {
    return System.currentTimeMillis() / 1000;
  }

  public static String hash(String value) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public synchronized void close() throws Exception {
    try {
      db.close();
    } finally {
      lock.release();
      lockChannel.close();
    }
  }
}
