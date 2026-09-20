package service

import (
	"context"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

//go:embed operations.json
var operationJSON []byte

type Operation struct {
	Route      string `json:"route"`
	Method     string `json:"method"`
	Module     string `json:"module"`
	Mode       string `json:"mode"`
	Capability string `json:"capability"`
}

var operations = func() map[string]Operation {
	var list []Operation
	if e := json.Unmarshal(operationJSON, &list); e != nil {
		panic(e)
	}
	m := map[string]Operation{}
	for _, op := range list {
		m[op.Route] = op
	}
	return m
}()

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func errorJSON(w http.ResponseWriter, status int, code, message string, extras map[string]any) {
	body := map[string]any{"code": code, "message": message}
	for k, v := range extras {
		body[k] = v
	}
	writeJSON(w, status, map[string]any{"error": body})
}
func (s *Service) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }
func (s *Service) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" && r.Method == "GET" {
		s.health(w)
		return
	}
	if s.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) != 1 {
		errorJSON(w, 401, "UNAUTHORIZED", "bearer token required", nil)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/v1/devices" {
		devices := make([]Device, 0, len(s.devices))
		for _, d := range s.devices {
			devices = append(devices, d)
		}
		sort.Slice(devices, func(i, j int) bool {
			return devices[i].ConnectionID+devices[i].DeviceID < devices[j].ConnectionID+devices[j].DeviceID
		})
		writeJSON(w, 200, map[string]any{"devices": devices})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) < 6 || parts[0] != "v1" || parts[1] != "connections" || parts[3] != "devices" {
		errorJSON(w, 404, "NOT_FOUND", "route not found", nil)
		return
	}
	d, ok := s.devices[parts[2]+"/"+parts[4]]
	if !ok {
		errorJSON(w, 404, "NOT_FOUND", "device not configured", nil)
		return
	}
	route := strings.Join(parts[5:], "/")
	if r.Method == "GET" && route == "latest" {
		s.latestHTTP(w, d)
		return
	}
	if r.Method == "GET" && route == "alarms/local" {
		s.alarmsHTTP(w, d)
		return
	}
	op, ok := operations[route]
	if !ok || r.Method != "POST" {
		errorJSON(w, 404, "NOT_FOUND", "route not found", nil)
		return
	}
	if op.Capability != "-" {
		found := false
		for _, capability := range d.Capabilities {
			if capability == op.Capability {
				found = true
			}
		}
		if !found {
			errorJSON(w, 409, "UNSUPPORTED_CAPABILITY", "operation requires configured capability: "+op.Capability, nil)
			return
		}
	}
	var raw map[string]json.RawMessage
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decodeErr := decoder.Decode(&raw)
	if errors.Is(decodeErr, io.EOF) {
		raw = map[string]json.RawMessage{}
	}
	if (decodeErr != nil && !errors.Is(decodeErr, io.EOF)) || raw == nil {
		errorJSON(w, 400, "INVALID_ARGUMENT", "body must be a JSON object up to 1 MiB", nil)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		errorJSON(w, 400, "INVALID_ARGUMENT", "body must contain exactly one object", nil)
		return
	}
	params := map[string]any{}
	for k, v := range raw {
		if k != "params" {
			errorJSON(w, 400, "INVALID_ARGUMENT", "unknown top-level field", nil)
			return
		}
		pd := json.NewDecoder(strings.NewReader(string(v)))
		pd.UseNumber()
		if e := pd.Decode(&params); e != nil || params == nil {
			errorJSON(w, 400, "INVALID_ARGUMENT", "params must be an object", nil)
			return
		}
	}
	codec := sdk.Codec{Format: sdk.PayloadFormat(d.Format)}
	if _, _, e := codec.EncodeRPC(op.Method, params, 1); e != nil {
		errorJSON(w, 400, "INVALID_ARGUMENT", e.Error(), nil)
		return
	}
	client := s.clients[d.ConnectionID+"/"+d.DeviceID]
	if client == nil || !client.Connected() {
		errorJSON(w, 503, "UNAVAILABLE", "device MQTT connection is unavailable", nil)
		return
	}
	select {
	case s.rpcSlots <- struct{}{}:
		defer func() { <-s.rpcSlots }()
	default:
		errorJSON(w, 503, "OVERLOADED", "RPC concurrency limit reached", nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.config.RPCTimeoutMs)*time.Millisecond)
	defer cancel()
	response, e := s.invokeDomain(ctx, d, op, params)
	if e != nil {
		var rpc *sdk.RPCError
		var invalid *InvalidArgument
		switch {
		case errors.As(e, &invalid):
			errorJSON(w, 400, "INVALID_ARGUMENT", invalid.Message, nil)
		case errors.Is(e, context.DeadlineExceeded):
			errorJSON(w, 504, "RPC_TIMEOUT", "device response timed out; outcome unknown", map[string]any{"outcome": "unknown"})
		case errors.As(e, &rpc):
			errorJSON(w, 502, "DEVICE_ERROR", "device rejected RPC", map[string]any{"deviceCode": rpc.Code})
		default:
			errorJSON(w, 503, "UNAVAILABLE", "device transport unavailable", map[string]any{"outcome": "unknown"})
		}
		return
	}
	fields, e := response.AsMap()
	if e != nil {
		errorJSON(w, 502, "DEVICE_ERROR", "invalid device response", nil)
		return
	}
	status, word := 200, "completed"
	if op.Mode == "async" {
		status, word = 202, "accepted"
	}
	writeJSON(w, status, map[string]any{"connectionId": d.ConnectionID, "deviceId": d.DeviceID, "method": op.Method, "status": word, "response": fields})
}
func (s *Service) health(w http.ResponseWriter) {
	status := "ok"
	connections := make([]map[string]any, 0, len(s.devices))
	for key, d := range s.devices {
		c := s.clients[key]
		connected := c != nil && c.Connected()
		if !connected {
			status = "degraded"
		}
		connections = append(connections, map[string]any{"connectionId": d.ConnectionID, "deviceId": d.DeviceID, "connected": connected})
	}
	counters := map[string]int64{"ingestFailures": s.ingestFailures.Load()}
	for name, q := range map[string]string{"inboxPending": "SELECT count(*) FROM inbox WHERE status='pending'", "inboxFailed": "SELECT count(*) FROM inbox WHERE status='failed'", "outboxPending": "SELECT count(*) FROM outbox WHERE status='pending'", "outboxFailed": "SELECT count(*) FROM outbox WHERE status='failed'", "jobsPending": "SELECT count(*) FROM jobs WHERE status='pending'", "jobsFailed": "SELECT count(*) FROM jobs WHERE status='failed'"} {
		var n int64
		if e := s.db.QueryRow(q).Scan(&n); e != nil {
			status = "degraded"
			counters["databaseErrors"]++
		}
		counters[name] = n
		if strings.HasSuffix(name, "Failed") && n > 0 {
			status = "degraded"
		}
	}
	writeJSON(w, 200, map[string]any{"status": status, "connections": connections, "counters": counters})
}
func (s *Service) latestHTTP(w http.ResponseWriter, d Device) {
	var telemetry, attributes sql.NullString
	var count int64
	e := s.db.QueryRow("SELECT telemetry,attributes,received_count FROM latest WHERE connection=? AND device=?", d.ConnectionID, d.DeviceID).Scan(&telemetry, &attributes, &count)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		errorJSON(w, 503, "UNAVAILABLE", "database unavailable", nil)
		return
	}
	var t, a any
	if telemetry.Valid {
		_ = json.Unmarshal([]byte(telemetry.String), &t)
	}
	if attributes.Valid {
		_ = json.Unmarshal([]byte(attributes.String), &a)
	}
	writeJSON(w, 200, map[string]any{"telemetry": t, "attributes": a, "receivedCount": count})
}
func (s *Service) alarmsHTTP(w http.ResponseWriter, d Device) {
	rows, e := s.db.Query("SELECT payload,needs_reconcile FROM alarms WHERE connection=? AND device=? ORDER BY alarm_id", d.ConnectionID, d.DeviceID)
	if e != nil {
		errorJSON(w, 503, "UNAVAILABLE", "database unavailable", nil)
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var p string
		var reconcile int
		if e = rows.Scan(&p, &reconcile); e != nil {
			break
		}
		var f map[string]any
		if e = json.Unmarshal([]byte(p), &f); e != nil {
			break
		}
		f["needsReconcile"] = reconcile != 0
		items = append(items, f)
	}
	if e != nil || rows.Err() != nil {
		errorJSON(w, 503, "UNAVAILABLE", "database unavailable", nil)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
