package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (s *Service) DeliverOne(ctx context.Context) error {
	if s.webhook == "" {
		return sql.ErrNoRows
	}
	var key, payload string
	var attempts int
	e := s.db.QueryRowContext(ctx, "SELECT key,payload,attempts FROM outbox WHERE status='pending' AND next_at<=? ORDER BY created,key LIMIT 1", time.Now().Unix()).Scan(&key, &payload, &attempts)
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", s.webhook, bytes.NewBufferString(payload))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	if s.webhookToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.webhookToken)
	}
	response, e := s.httpClient.Do(req)
	if e == nil {
		io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			e = fmt.Errorf("webhook HTTP status %d", response.StatusCode)
		}
	}
	return s.finishWork("outbox", key, attempts, e)
}
func (s *Service) finishWork(table, key string, attempts int, cause error) error {
	status, next, detail := "done", int64(0), ""
	if cause != nil {
		attempts++
		status = "pending"
		if attempts >= 8 {
			status = "failed"
		}
		next = time.Now().Unix() + int64(1<<min(attempts, 8))
		detail = cause.Error()
	}
	_, e := s.db.Exec("UPDATE "+table+" SET status=?,attempts=?,next_at=?,error=? WHERE key=?", status, attempts, next, detail, key)
	if e != nil {
		return e
	}
	return cause
}
func (s *Service) RunJobOne(ctx context.Context) error {
	var key, connection, device, kind, params string
	var attempts int
	e := s.db.QueryRowContext(ctx, "SELECT key,connection,device,kind,params,attempts FROM jobs WHERE status='pending' AND next_at<=? ORDER BY created,key LIMIT 1", time.Now().Unix()).Scan(&key, &connection, &device, &kind, &params, &attempts)
	if e != nil {
		return e
	}
	client := s.clients[connection+"/"+device]
	if client == nil || !client.Connected() { // Broker outage does not consume the finite retry budget.
		_, e = s.db.Exec("UPDATE jobs SET next_at=? WHERE key=?", time.Now().Unix()+5, key)
		return e
	}
	var p map[string]any
	if e = json.Unmarshal([]byte(params), &p); e != nil {
		return s.finishWork("jobs", key, attempts, e)
	}
	var revisions map[string]alarmRevision
	method := "ackEvidencePackage"
	if kind == "reconcile" {
		method = "getAlarmState"
		revisions, e = s.alarmRevisions(connection, device)
		if e != nil {
			return s.finishWork("jobs", key, attempts, e)
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, time.Duration(s.config.RPCTimeoutMs)*time.Millisecond)
	defer cancel()
	response, e := client.Call(callCtx, method, p, 0, false)
	if e != nil {
		return s.finishWork("jobs", key, attempts, e)
	}
	if kind == "reconcile" {
		fields, e := response.AsMap()
		if e != nil {
			return s.finishWork("jobs", key, attempts, e)
		}
		if e = s.reconcile(connection, device, fields, revisions); e != nil {
			return s.finishWork("jobs", key, attempts, e)
		}
	}
	return s.finishWork("jobs", key, attempts, nil)
}

// Reconciliation stores the authoritative state separately from event timestamp
// ordering. It never invents transitions or generates downstream notifications.
func (s *Service) reconcile(connection, device string, response map[string]any, revisions map[string]alarmRevision) error {
	body := response
	if value, ok := response["getAlarmState"].(map[string]any); ok {
		body = value
	} else if value, ok := response["data"].(map[string]any); ok {
		body = value
	}
	active, ok := body["active"].([]any)
	if !ok { // ProtoJSON omits an empty repeated field.
		if _, present := body["active"]; present {
			return errors.New("invalid active alarm response")
		}
		active = []any{}
	}
	states := map[string]any{}
	for _, item := range active {
		f, ok := item.(map[string]any)
		if !ok {
			return errors.New("invalid active alarm")
		}
		id, ok := f["alarmId"].(string)
		if !ok {
			return errors.New("missing active alarm ID")
		}
		states[id] = f
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for id, revision := range revisions {
		var f map[string]any
		if e = json.Unmarshal([]byte(revision.payload), &f); e != nil {
			return e
		}
		f["reconciledAt"] = time.Now().Unix()
		f["currentState"] = states[id]
		_, f["currentActive"] = states[id]
		b, e := json.Marshal(f)
		if e != nil {
			return e
		}
		// Newer or equal timestamp arrivals while the RPC was in flight remain dirty.
		if _, e = tx.Exec("UPDATE alarms SET payload=?,needs_reconcile=0 WHERE connection=? AND device=? AND alarm_id=? AND payload=? AND needs_reconcile=?", string(b), connection, device, id, revision.payload, revision.version); e != nil {
			return e
		}
	}
	return tx.Commit()
}

type alarmRevision struct {
	payload string
	version int
}

func (s *Service) alarmRevisions(connection, device string) (map[string]alarmRevision, error) {
	rows, e := s.db.Query("SELECT alarm_id,payload,needs_reconcile FROM alarms WHERE connection=? AND device=? AND needs_reconcile>0", connection, device)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := map[string]alarmRevision{}
	for rows.Next() {
		var id string
		var revision alarmRevision
		if e = rows.Scan(&id, &revision.payload, &revision.version); e != nil {
			return nil, e
		}
		result[id] = revision
	}
	return result, rows.Err()
}
