package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeClient struct {
	calls  int
	method string
	err    error
	onCall func()
}

func (f *fakeClient) Call(_ context.Context, method string, _ any, _ int32, _ bool) (*sdk.DecodedPayload, error) {
	f.calls++
	f.method = method
	if f.onCall != nil {
		f.onCall()
	}
	if f.err != nil {
		return nil, f.err
	}
	return &sdk.DecodedPayload{Value: map[string]any{"code": 0}}, nil
}
func (*fakeClient) Connected() bool { return true }
func testService(t *testing.T) *Service {
	t.Helper()
	c := DefaultConfig()
	c.Connections = []ConnectionConfig{{ID: "local", Host: "localhost", Port: 1883, Devices: []DeviceConfig{{ID: "DEMO", Format: "json"}}}}
	s, e := New(c, t.TempDir(), "", "", "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	s.clients["local/DEMO"] = &fakeClient{}
	return s
}
func request(s *Service, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(method, path, strings.NewReader(body)))
	return r
}
func TestRESTContract(t *testing.T) {
	s := testService(t)
	cases := []struct {
		path, body string
		status     int
	}{{"device/reboot", "{}", 202}, {"targets/query", "{\"params\":{}}", 200}, {"device/motor/enable", "{}", 409}, {"device/reboot", "[]", 400}, {"device/reboot", "{\"params\":[]}", 400}, {"device/reboot", "{\"extra\":1}", 400}, {"device/reboot", "{} {}", 400}, {"rpc", "{}", 404}}
	for _, c := range cases {
		r := request(s, "POST", "/v1/connections/local/devices/DEMO/"+c.path, c.body)
		if r.Code != c.status {
			t.Errorf("%s %s: %d %s", c.path, c.body, r.Code, r.Body)
		}
	}
	f := s.clients["local/DEMO"].(*fakeClient)
	before := f.calls
	if r := request(s, "POST", "/v1/connections/local/devices/OTHER/device/reboot", "{}"); r.Code != 404 || f.calls != before {
		t.Fatal("unknown device called SDK")
	}
	f.err = context.DeadlineExceeded
	if r := request(s, "POST", "/v1/connections/local/devices/DEMO/device/reboot", "{}"); r.Code != 504 || !strings.Contains(r.Body.String(), "unknown") {
		t.Fatal(r.Code, r.Body)
	}
	f.err = &sdk.RPCError{Code: 300}
	if r := request(s, "POST", "/v1/connections/local/devices/DEMO/device/reboot", "{}"); r.Code != 502 {
		t.Fatal(r.Code, r.Body)
	}
}
func TestAuthenticationAndBodyLimit(t *testing.T) {
	s := testService(t)
	s.token = "secret"
	if r := request(s, "GET", "/v1/devices", ""); r.Code != 401 {
		t.Fatal(r.Code)
	}
	if r := request(s, "GET", "/health", ""); r.Code != 200 {
		t.Fatal(r.Code)
	}
	s.token = ""
	if r := request(s, "POST", "/v1/connections/local/devices/DEMO/device/reboot", `{"params":{"x":"`+strings.Repeat("x", 1<<20)+`"}}`); r.Code != 400 {
		t.Fatal(r.Code)
	}
}
func TestDurableBoundedInboxAndLatest(t *testing.T) {
	s := testService(t)
	s.config.MaxInboxRows = 1
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/telemetry", []byte(`{"ts":1}`)); e != nil {
		t.Fatal(e)
	}
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/telemetry", []byte(`{"ts":2}`)); !errors.Is(e, ErrOverloaded) {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	r := request(s, "GET", "/v1/connections/local/devices/DEMO/latest", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"receivedCount":1`) {
		t.Fatal(r.Code, r.Body)
	}
}
func alarm(t *testing.T, s *Service, id, transition string, ts int64) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"eventId": id, "alarmId": "9", "transition": transition, "ts": ts})
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/3A", b); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func count(t *testing.T, s *Service, table string) int {
	t.Helper()
	var n int
	if e := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestAlarmDedupOrderingAndNotification(t *testing.T) {
	s := testService(t)
	now := time.Now().Unix()
	alarm(t, s, "1", "ALARM_TRANSITION_TRIGGERED", now-2)
	alarm(t, s, "1", "TRIGGERED", now-2)
	if n := count(t, s, "outbox"); n != 1 {
		t.Fatal(n)
	}
	alarm(t, s, "2", "ESCALATED", now-2)
	if count(t, s, "outbox") != 1 || count(t, s, "jobs") != 1 {
		t.Fatal("equal timestamp must reconcile without notification")
	}
	alarm(t, s, "3", "DEESCALATED", now-1)
	alarm(t, s, "4", "SYNCED", now)
	alarm(t, s, "5", "RECOVERED", now+99)
	if count(t, s, "outbox") != 1 {
		t.Fatal("suppression failed")
	}
	r := request(s, "GET", "/v1/connections/local/devices/DEMO/alarms/local", "")
	if !strings.Contains(r.Body.String(), "needsReconcile") {
		t.Fatal(r.Body)
	}
}
func TestEventNeverCreatesAlarmNotification(t *testing.T) {
	s := testService(t)
	for _, state := range []string{"READY", "SENT"} {
		b, _ := json.Marshal(map[string]any{"eventId": "1", "state": state})
		if e := s.Ingest("local", "DEMO", "vdm/DEMO/event", b); e != nil {
			t.Fatal(e)
		}
		if e := s.ProcessOne(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if count(t, s, "evidence_events") != 2 || count(t, s, "outbox") != 0 {
		t.Fatal("evidence lifecycle conflated with alarm")
	}
}
func TestOutboxRetryAndIdempotency(t *testing.T) {
	s := testService(t)
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(keys) == 1 {
			w.WriteHeader(503)
		}
	}))
	defer server.Close()
	s.webhook = server.URL
	alarm(t, s, "1", "TRIGGERED", time.Now().Unix())
	s.DeliverOne(context.Background())
	s.db.Exec("UPDATE outbox SET next_at=0")
	s.DeliverOne(context.Background())
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatal(keys)
	}
	var status string
	s.db.QueryRow("SELECT status FROM outbox").Scan(&status)
	if status != "done" {
		t.Fatal(status)
	}
}
func TestConfigRejectsDuplicateAndUnsafeBind(t *testing.T) {
	s := testService(t)
	c := s.config
	c.Connections = append(c.Connections, c.Connections[0])
	if c.Validate() == nil {
		t.Fatal("duplicate connection accepted")
	}
	if ValidateBind("0.0.0.0", "") == nil {
		t.Fatal("unsafe bind accepted")
	}
	if ValidateBind("127.0.0.1", "") != nil {
		t.Fatal("loopback rejected")
	}
}

func TestRESTEvidenceACKRequiresLocalVerifiedReceipt(t *testing.T) {
	s := testService(t)
	f := s.clients["local/DEMO"].(*fakeClient)
	r := request(s, "POST", "/v1/connections/local/devices/DEMO/evidence/ack", `{"params":{"eventId":"42","kind":"SNAPSHOT","packageSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
	if r.Code != 400 || f.calls != 0 {
		t.Fatal(r.Code, r.Body, f.calls)
	}
}

func TestReconcileDoesNotClearNewerAmbiguity(t *testing.T) {
	s := testService(t)
	now := time.Now().Unix()
	alarm(t, s, "1", "TRIGGERED", now)
	alarm(t, s, "2", "ESCALATED", now)
	f := s.clients["local/DEMO"].(*fakeClient)
	f.onCall = func() { alarm(t, s, "3", "RECOVERED", now) }
	if e := s.RunJobOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	var dirty int
	s.db.QueryRow("SELECT needs_reconcile FROM alarms").Scan(&dirty)
	if dirty == 0 {
		t.Fatal("in-flight query cleared a newer ambiguous event")
	}
}

func TestReconciledAlarmUsesSharedResponseFields(t *testing.T) {
	s := testService(t)
	now := time.Now().Unix()
	alarm(t, s, "1", "TRIGGERED", now)
	alarm(t, s, "2", "RECOVERED", now)
	if e := s.RunJobOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	r := request(s, "GET", "/v1/connections/local/devices/DEMO/alarms/local", "")
	var body struct {
		Items []map[string]any `json:"items"`
	}
	if e := json.Unmarshal(r.Body.Bytes(), &body); e != nil {
		t.Fatal(e)
	}
	item := body.Items[0]
	if item["currentActive"] != false {
		t.Fatal("missing canonical currentActive", item)
	}
	if current, present := item["currentState"]; !present || current != nil {
		t.Fatal("missing null canonical currentState", item)
	}
	if item["needsReconcile"] != false || item["transition"] != "TRIGGERED" || item["reconciledAt"] == nil {
		t.Fatal("lost event or reconciliation metadata", item)
	}
}

func TestRESTEmptyBodyUsesEmptyParams(t *testing.T) {
	s := testService(t)
	r := request(s, "POST", "/v1/connections/local/devices/DEMO/device/attributes/query", "")
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body)
	}
}
func TestOnlyFirstTriggerAndHigherLevelEscalationNotify(t *testing.T) {
	s := testService(t)
	now := time.Now().Unix() - 10
	for i, event := range []struct{ transition, level string }{{"TRIGGERED", "ALERT"}, {"TRIGGERED", "ALERT"}, {"ESCALATED", "ALERT"}, {"ESCALATED", "ALARM"}, {"RECOVERED", ""}, {"CANCELLED", ""}} {
		f := map[string]any{"eventId": fmt.Sprint(i + 1), "alarmId": "9", "ts": now + int64(i), "transition": event.transition}
		if event.level != "" {
			f["level"] = event.level
		}
		raw, _ := json.Marshal(f)
		s.Ingest("local", "DEMO", "vdm/DEMO/3A", raw)
		if e := s.ProcessOne(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if n := count(t, s, "outbox"); n != 4 {
		t.Fatal("repeat trigger/non-increasing escalation notified", n)
	}
}
func TestProcessedHistoryCapacityPreservesPendingWork(t *testing.T) {
	s := testService(t)
	s.config.MaxInboxRows = 1
	now := time.Now().Unix() - 100
	for i := 0; i < 15; i++ {
		alarm(t, s, fmt.Sprint(i+1), "SYNCED", now+int64(i))
		payload := []byte(fmt.Sprintf(`{"eventId":"%d","state":"READY"}`, i))
		s.Ingest("local", "DEMO", "vdm/DEMO/event", payload)
		if e := s.ProcessOne(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	s.Ingest("local", "DEMO", "vdm/DEMO/telemetry", []byte(`{"pending":true}`))
	if e := s.Prune(); e != nil {
		t.Fatal(e)
	}
	if count(t, s, "alarm_events") != 10 || count(t, s, "evidence_events") != 10 {
		t.Fatal("processed history unbounded")
	}
	var pending int
	s.db.QueryRow("SELECT count(*) FROM inbox WHERE status='pending'").Scan(&pending)
	if pending != 1 {
		t.Fatal("pending work pruned")
	}
}

func TestFullOutboxRollsBackAlarmAndRetainsInboxForRetry(t *testing.T) {
	s := testService(t)
	s.config.MaxInboxRows = 1
	now := time.Now().Unix() - 10
	alarm(t, s, "1", "TRIGGERED", now)
	// A terminal failed notification still occupies its durable capacity slot.
	if _, e := s.db.Exec("UPDATE outbox SET status='failed'"); e != nil {
		t.Fatal(e)
	}
	body, _ := json.Marshal(map[string]any{"eventId": "2", "alarmId": "9", "ts": now + 1, "transition": "RECOVERED"})
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/3A", body); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); !errors.Is(e, ErrOverloaded) {
		t.Fatal("full outbox should backpressure inbox", e)
	}
	if count(t, s, "alarm_events") != 1 || count(t, s, "outbox") != 1 {
		t.Fatal("full outbox did not roll back event")
	}
	var payload, status string
	if e := s.db.QueryRow("SELECT payload FROM alarms").Scan(&payload); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(payload, `"transition":"TRIGGERED"`) {
		t.Fatal("state advanced without notification", payload)
	}
	if e := s.db.QueryRow("SELECT status FROM inbox ORDER BY id DESC LIMIT 1").Scan(&status); e != nil || status != "pending" {
		t.Fatal("work not retained", status, e)
	}
	// Operator resolves the previous terminal failure. The original raw message
	// can now commit exactly one lifecycle event and notification without replay.
	s.db.Exec("UPDATE outbox SET status='done'")
	s.db.Exec("UPDATE inbox SET next_at=0")
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	if count(t, s, "alarm_events") != 2 {
		t.Fatal("retry did not commit event")
	}
	var pending int
	s.db.QueryRow("SELECT count(*) FROM outbox WHERE status IN ('pending','failed')").Scan(&pending)
	if pending != 1 {
		t.Fatal("wrong active outbox count", pending)
	}
}

func TestFullBackgroundJobsRollsBackAndRetriesAmbiguousEvent(t *testing.T) {
	s := testService(t)
	s.config.MaxInboxRows = 1
	now := time.Now().Unix()
	alarm(t, s, "1", "SYNCED", now)
	alarm(t, s, "2", "SYNCED", now)
	raw, _ := json.Marshal(map[string]any{"eventId": "3", "alarmId": "9", "transition": "SYNCED", "ts": now})
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/3A", raw); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); !errors.Is(e, ErrOverloaded) {
		t.Fatal("unbounded job insertion", e)
	}
	if count(t, s, "jobs") != 1 || count(t, s, "alarm_events") != 2 {
		t.Fatal("job overload not atomic")
	}
	if e := s.RunJobOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	s.db.Exec("UPDATE inbox SET next_at=0")
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	if count(t, s, "alarm_events") != 3 {
		t.Fatal("retained event not retried")
	}
	var occupied int
	s.db.QueryRow("SELECT count(*) FROM jobs WHERE status IN ('pending','failed')").Scan(&occupied)
	if occupied != 1 {
		t.Fatal("job cap violated", occupied)
	}
}
