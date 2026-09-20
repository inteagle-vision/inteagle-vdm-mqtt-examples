package service

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func enum(v any, prefix string) string { return strings.TrimPrefix(fmt.Sprint(v), prefix) }
func uintID(v any) (string, error) {
	s, ok := v.(string)
	if !ok || s == "" {
		return "", errors.New("alarm IDs must be decimal strings")
	}
	if n, e := strconv.ParseUint(s, 10, 64); e != nil || n == 0 {
		return "", errors.New("invalid alarm ID")
	}
	return s, nil
}
func timestamp(v any) (int64, error) {
	switch n := v.(type) {
	case string:
		return strconv.ParseInt(n, 10, 64)
	case float64:
		if n != float64(int64(n)) {
			return 0, errors.New("timestamp must be integer")
		}
		return int64(n), nil
	case json.Number:
		return n.Int64()
	default:
		return 0, errors.New("missing alarm timestamp")
	}
}
func (s *Service) storeAlarm(tx *sql.Tx, row inboxItem, f map[string]any) error {
	event, e := uintID(f["eventId"])
	if e != nil {
		return e
	}
	alarm, e := uintID(f["alarmId"])
	if e != nil {
		return e
	}
	ts, e := timestamp(f["ts"])
	if e != nil || ts < 0 {
		return errors.New("invalid alarm timestamp")
	}
	transition := enum(f["transition"], "ALARM_TRANSITION_")
	switch transition {
	case "TRIGGERED", "ESCALATED", "RECOVERED", "CANCELLED", "DEESCALATED", "SYNCED":
	default:
		return errors.New("unsupported alarm transition")
	}
	f["transition"] = transition
	if level, ok := f["level"]; ok {
		f["level"] = enum(level, "ALARM_LEVEL_")
	}
	if typ, ok := f["alarmType"]; ok {
		f["alarmType"] = enum(typ, "ALARM_TYPE_")
	}
	payload, e := json.Marshal(f)
	if e != nil {
		return e
	}
	res, e := tx.Exec("INSERT OR IGNORE INTO alarm_events(connection,device,event_id,alarm_id,ts,payload,created) VALUES(?,?,?,?,?,?,?)", row.connection, row.device, event, alarm, ts, string(payload), time.Now().Unix())
	if e != nil {
		return e
	}
	inserted, e := res.RowsAffected()
	if e != nil || inserted == 0 {
		return e
	}
	var oldTS int64
	var oldPayload string
	e = tx.QueryRow("SELECT ts,payload FROM alarms WHERE connection=? AND device=? AND alarm_id=?", row.connection, row.device, alarm).Scan(&oldTS, &oldPayload)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	hadPrior := e == nil
	if hadPrior && ts <= oldTS {
		if _, e = tx.Exec("UPDATE alarms SET needs_reconcile=needs_reconcile+1 WHERE connection=? AND device=? AND alarm_id=?", row.connection, row.device, alarm); e != nil {
			return e
		}
		return s.scheduleJob(tx, row.connection, row.device, "reconcile", map[string]any{}, stableKey(row.connection, row.device, event, "reconcile"), false)
	}
	_, e = tx.Exec("INSERT INTO alarms(connection,device,alarm_id,ts,payload) VALUES(?,?,?,?,?) ON CONFLICT(connection,device,alarm_id) DO UPDATE SET ts=excluded.ts,payload=excluded.payload", row.connection, row.device, alarm, ts, string(payload))
	if e != nil {
		return e
	}
	eligible := transition == "RECOVERED" || transition == "CANCELLED"
	if transition == "TRIGGERED" {
		eligible = !hadPrior
	}
	if transition == "ESCALATED" {
		eligible = !hadPrior
		if hadPrior {
			var prior map[string]any
			if e = json.Unmarshal([]byte(oldPayload), &prior); e != nil {
				return e
			}
			ranks := map[string]int{"ALERT": 1, "ALARM": 2, "ACTION": 3}
			eligible = ranks[fmt.Sprint(f["level"])] > ranks[fmt.Sprint(prior["level"])]
		}
	}
	now := time.Now().Unix()
	if !eligible {
		return nil
	}
	if transition == "SYNCED" || transition == "DEESCALATED" || ts > now || now-ts > s.config.NotificationMaxAgeSeconds {
		return nil
	}
	// Capacity is checked inside the same transaction as event/state/outbox.
	// Failed deliveries retain their slot until explicitly resolved; pending work
	// is never evicted, including when no webhook URL has been configured.
	var occupied int
	if e = tx.QueryRow("SELECT count(*) FROM outbox WHERE status IN ('pending','failed')").Scan(&occupied); e != nil {
		return e
	}
	if occupied >= s.config.MaxInboxRows {
		return ErrOverloaded
	}
	body, _ := json.Marshal(map[string]any{"connectionId": row.connection, "deviceId": row.device, "eventId": event, "alarmId": alarm, "transition": transition, "alarm": f})
	_, e = tx.Exec("INSERT OR IGNORE INTO outbox(key,connection,device,payload,created) VALUES(?,?,?,?,?)", stableKey(row.connection, row.device, event, transition), row.connection, row.device, string(body), now)
	return e
}
func (s *Service) scheduleJob(tx *sql.Tx, connection, device, kind string, params any, key string, reopen bool) error {
	var status string
	e := tx.QueryRow("SELECT status FROM jobs WHERE key=?", key).Scan(&status)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if e == nil && !reopen {
		return nil
	}
	if errors.Is(e, sql.ErrNoRows) || status == "done" {
		var occupied int
		if e = tx.QueryRow("SELECT count(*) FROM jobs WHERE status IN ('pending','failed')").Scan(&occupied); e != nil {
			return e
		}
		if occupied >= s.config.MaxInboxRows {
			return ErrOverloaded
		}
	}
	payload, e := json.Marshal(params)
	if e != nil {
		return e
	}
	q := "INSERT INTO jobs(key,connection,device,kind,params,created) VALUES(?,?,?,?,?,?) ON CONFLICT(key) DO NOTHING"
	if reopen {
		q = "INSERT INTO jobs(key,connection,device,kind,params,created) VALUES(?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status='pending',attempts=0,next_at=0,created=excluded.created"
	}
	_, e = tx.Exec(q, key, connection, device, kind, string(payload), time.Now().Unix())
	return e
}
