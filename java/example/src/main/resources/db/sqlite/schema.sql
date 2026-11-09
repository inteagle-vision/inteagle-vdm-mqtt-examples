-- V1：客户接入演示的 SQLite schema。由 SqliteDatabase 在 BEGIN IMMEDIATE 事务内执行。
-- 不依赖 Spring/JPA/Flyway。STRICT 保证列类型，复合主键保证 QoS 1 重放幂等。
-- 所有时间为 UTC：采样时间使用 Unix 毫秒，运维时间使用 SQLite CURRENT_TIMESTAMP。
-- 已发布的 V1 文件不可原地修改。后续结构变更必须新增版本迁移并更新加载器。

CREATE TABLE schema_history (
  version INTEGER PRIMARY KEY CHECK (version > 0),
  description TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL CHECK (length(checksum_sha256) = 64),
  installed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

-- 一个 eventId 表示一次状态变化，而非整个告警生命周期。uint64 用 TEXT 防止溢出。
CREATE TABLE alarm_events (
  device_id TEXT NOT NULL CHECK (length(trim(device_id)) > 0),
  event_id TEXT NOT NULL CHECK (length(event_id) BETWEEN 1 AND 20 AND event_id NOT GLOB '*[^0-9]*' AND substr(event_id,1,1) BETWEEN '1' AND '9' AND (length(event_id) < 20 OR event_id <= '18446744073709551615')),
  alarm_id TEXT NOT NULL CHECK (length(alarm_id) BETWEEN 1 AND 20 AND alarm_id NOT GLOB '*[^0-9]*' AND substr(alarm_id,1,1) BETWEEN '1' AND '9' AND (length(alarm_id) < 20 OR alarm_id <= '18446744073709551615')),
  transition TEXT NOT NULL CHECK (transition IN ('TRIGGERED','ESCALATED','DEESCALATED','SYNCED','RECOVERED','CANCELLED')),
  received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (device_id, event_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_alarm_events_incident ON alarm_events(device_id, alarm_id, received_at);

-- 生命周期状态：终态必须清空等级，活动态必须使用公开的三级枚举。
CREATE TABLE alarm_incidents (
  device_id TEXT NOT NULL,
  alarm_id TEXT NOT NULL CHECK (length(alarm_id) BETWEEN 1 AND 20 AND alarm_id NOT GLOB '*[^0-9]*' AND substr(alarm_id,1,1) BETWEEN '1' AND '9' AND (length(alarm_id) < 20 OR alarm_id <= '18446744073709551615')),
  active INTEGER NOT NULL CHECK (active IN (0,1)),
  level TEXT,
  last_event_id TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (device_id, alarm_id),
  FOREIGN KEY (device_id, last_event_id) REFERENCES alarm_events(device_id, event_id),
  CHECK ((active = 0 AND level IS NULL) OR (active = 1 AND level IS NOT NULL AND level IN ('ALERT','ALARM','ACTION')))
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_alarm_incidents_active ON alarm_incidents(device_id, updated_at) WHERE active = 1;

-- 通知任务与事件/生命周期在同一事务提交。外部发送使用稳定幂等键，不能在入库前发送。
CREATE TABLE notification_outbox (
  device_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  notification TEXT NOT NULL CHECK (notification IN ('ALARM_TRIGGERED','ALARM_ESCALATED','ALARM_RECOVERED','ALARM_CANCELLED')),
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENDING','SENT')),
  delivery_token TEXT,
  locked_until TEXT,
  next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error TEXT CHECK (length(last_error) <= 500),
  sent_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (device_id, event_id, notification),
  FOREIGN KEY (device_id, event_id) REFERENCES alarm_events(device_id, event_id),
  CHECK ((status = 'SENDING' AND delivery_token IS NOT NULL AND length(delivery_token) > 0 AND locked_until IS NOT NULL)
      OR (status != 'SENDING' AND delivery_token IS NULL AND locked_until IS NULL)),
  CHECK ((status = 'SENT' AND sent_at IS NOT NULL) OR (status != 'SENT' AND sent_at IS NULL))
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_outbox_pending ON notification_outbox(next_attempt_at, created_at) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_expired ON notification_outbox(locked_until, created_at) WHERE status = 'SENDING';

-- 位移时序：一个标靶一个采样点一行，单位 mm。缺失轴为 NULL，不能补成 0。
-- 同一设备/标靶/采样时间的重放不重复插入。主键同时支持按设备、标靶查询时间范围。
CREATE TABLE displacement_samples (
  device_id TEXT NOT NULL CHECK (length(trim(device_id)) > 0),
  target_id TEXT NOT NULL CHECK (length(trim(target_id)) > 0),
  sample_time_ms INTEGER NOT NULL CHECK (sample_time_ms >= 0),
  batch_start_ms INTEGER NOT NULL CHECK (batch_start_ms >= 0),
  sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
  sample_frequency_hz INTEGER NOT NULL CHECK (sample_frequency_hz BETWEEN 1 AND 60),
  dx_mm REAL,
  dy_mm REAL,
  dz_mm REAL,
  received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (device_id, target_id, sample_time_ms),
  CHECK (dx_mm IS NOT NULL OR dy_mm IS NOT NULL OR dz_mm IS NOT NULL)
) STRICT, WITHOUT ROWID;
-- 按保留时间批量清理时，避免扫描每个设备/标靶的完整历史。
CREATE INDEX idx_displacement_retention ON displacement_samples(sample_time_ms);
