package service

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newAt(t *testing.T, dir string) *Service {
	t.Helper()
	c := DefaultConfig()
	c.Connections = []ConnectionConfig{{ID: "local", Host: "localhost", Port: 1883, Devices: []DeviceConfig{{ID: "DEMO", Format: "json"}}}}
	s, e := New(c, dir, "", "", "")
	if e != nil {
		t.Fatal(e)
	}
	s.clients["local/DEMO"] = &fakeClient{}
	return s
}
func TestRestartPendingInboxAndExclusiveDirectory(t *testing.T) {
	dir := t.TempDir()
	s := newAt(t, dir)
	if _, e := New(s.config, dir, "", "", ""); e == nil {
		t.Fatal("second instance accepted")
	}
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/telemetry", []byte(`{"sequence":7}`)); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s = newAt(t, dir)
	defer s.Close()
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(request(s, "GET", "/v1/connections/local/devices/DEMO/latest", "").Body.Bytes(), []byte(`"sequence":7`)) {
		t.Fatal("pending inbox lost")
	}
	var version int
	s.db.QueryRow("PRAGMA user_version").Scan(&version)
	if version != 1 {
		t.Fatal(version)
	}
}
func packageChunks(t *testing.T) [][]byte {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	for _, file := range []struct {
		name string
		data []byte
	}{{"manifest.json", []byte(`{"eventId":"42"}`)}, {"snapshot.jpg", bytes.Repeat([]byte{1}, 140000)}} {
		if e := tw.WriteHeader(&tar.Header{Name: file.name, Mode: 0600, Size: int64(len(file.data)), Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
		tw.Write(file.data)
	}
	tw.Close()
	raw := archive.Bytes()
	hash := sha256.Sum256(raw)
	var chunks [][]byte
	for offset := 0; offset < len(raw); offset += 128 * 1024 {
		size := min(128*1024, len(raw)-offset)
		p := make([]byte, 76+size)
		p[0], p[1], p[2], p[3] = 2, 76, 1, 1
		binary.BigEndian.PutUint64(p[4:12], 42)
		binary.BigEndian.PutUint64(p[12:20], uint64(len(raw)))
		copy(p[20:52], hash[:])
		binary.BigEndian.PutUint32(p[52:56], uint32(offset/(128*1024)))
		binary.BigEndian.PutUint32(p[56:60], uint32((len(raw)+128*1024-1)/(128*1024)))
		binary.BigEndian.PutUint64(p[60:68], uint64(offset))
		binary.BigEndian.PutUint32(p[68:72], uint32(size))
		copy(p[76:], raw[offset:offset+size])
		chunks = append(chunks, p)
	}
	return chunks
}
func TestEvidenceRestartAssemblyAndDurableACK(t *testing.T) {
	dir := t.TempDir()
	chunks := packageChunks(t)
	s := newAt(t, dir)
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/image", chunks[0]); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	if count(t, s, "jobs") != 0 || count(t, s, "evidence_chunks") != 1 {
		t.Fatal("premature ACK")
	}
	s.Close()
	s = newAt(t, dir)
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/image", chunks[1]); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	if count(t, s, "evidence_receipts") != 1 || count(t, s, "jobs") != 1 || count(t, s, "evidence_chunks") != 0 {
		t.Fatal("missing receipt/ACK work")
	}
	f := s.clients["local/DEMO"].(*fakeClient)
	f.err = context.DeadlineExceeded
	if e := s.RunJobOne(context.Background()); e == nil {
		t.Fatal("expected ack failure")
	}
	s.Close()
	s = newAt(t, dir)
	defer s.Close()
	s.db.Exec("UPDATE jobs SET next_at=0")
	if e := s.RunJobOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	f = s.clients["local/DEMO"].(*fakeClient)
	if f.method != "ackEvidencePackage" {
		t.Fatal(f.method)
	}
	var status, path string
	s.db.QueryRow("SELECT status FROM jobs").Scan(&status)
	s.db.QueryRow("SELECT path FROM evidence_receipts").Scan(&path)
	if status != "done" {
		t.Fatal(status)
	}
	if _, e := os.Stat(path); e != nil {
		t.Fatal(e)
	}
}
func TestEvidenceHashFailureNeverSchedulesACK(t *testing.T) {
	s := testService(t)
	chunks := packageChunks(t)
	for _, p := range chunks {
		p[20] ^= 1
		if e := s.Ingest("local", "DEMO", "vdm/DEMO/image", p); e != nil {
			t.Fatal(e)
		}
		_ = s.ProcessOne(context.Background())
	}
	if count(t, s, "jobs") != 0 || count(t, s, "evidence_receipts") != 0 {
		t.Fatal("corrupt package acknowledged")
	}
}
func TestStaleAlarmAndTerminalInboxFailure(t *testing.T) {
	s := testService(t)
	alarm(t, s, "1", "TRIGGERED", time.Now().Unix()-1000)
	if count(t, s, "outbox") != 0 {
		t.Fatal("stale notified")
	}
	s.Ingest("local", "DEMO", "vdm/DEMO/3A", []byte(`{"invalid":true}`))
	for i := 0; i < 8; i++ {
		s.db.Exec("UPDATE inbox SET next_at=0")
		_ = s.ProcessOne(context.Background())
	}
	var n int
	s.db.QueryRow("SELECT count(*) FROM inbox WHERE status='failed'").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
	if !bytes.Contains(request(s, "GET", "/health", "").Body.Bytes(), []byte(`"inboxFailed":1`)) {
		t.Fatal("failure invisible")
	}
}
func TestSharedOperationsStayInSync(t *testing.T) {
	raw, e := os.ReadFile("../../../contracts/operations.json")
	if e != nil {
		t.Fatal(e)
	}
	var expected []Operation
	if e = json.Unmarshal(raw, &expected); e != nil {
		t.Fatal(e)
	}
	if len(expected) != 33 || len(operations) != 33 {
		t.Fatal("wrong public operation count")
	}
	for _, op := range expected {
		if operations[op.Route] != op {
			t.Fatal("operation drift", op)
		}
	}
}

func TestLargeOrdinaryJPEGUsesEvidenceBudget(t *testing.T) {
	s := testService(t)
	payload := make([]byte, 2<<20)
	payload[0], payload[1], payload[8], payload[9] = 1, 8, 0xff, 0xd8
	if e := s.Ingest("local", "DEMO", "vdm/DEMO/image", payload); e != nil {
		t.Fatal(e)
	}
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	used, e := diskBytes(s.evidenceDir("local", "DEMO"))
	if e != nil || used != int64(len(payload)-8) {
		t.Fatal(used, e)
	}
}

func TestEvidenceRetentionProtectsPendingAndFailedThenInvalidatesReceipt(t *testing.T) {
	s := testService(t)
	for _, chunk := range packageChunks(t) {
		if e := s.Ingest("local", "DEMO", "vdm/DEMO/image", chunk); e != nil {
			t.Fatal(e)
		}
		if e := s.ProcessOne(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	var path, hash string
	if e := s.db.QueryRow("SELECT path,hash FROM evidence_receipts").Scan(&path, &hash); e != nil {
		t.Fatal(e)
	}
	old := time.Now().Add(-8 * 24 * time.Hour)
	s.db.Exec("UPDATE evidence_receipts SET created=?", old.Unix())
	s.db.Exec("UPDATE jobs SET created=?", old.Unix())
	for _, status := range []string{"pending", "failed"} {
		s.db.Exec("UPDATE jobs SET status=?", status)
		if e := s.Prune(); e != nil {
			t.Fatal(e)
		}
		if _, e := os.Stat(path); e != nil {
			t.Fatalf("%s package pruned: %v", status, e)
		}
	}
	s.db.Exec("UPDATE jobs SET status='done'")
	if e := s.Prune(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("expired acknowledged package still exists", e)
	}
	if count(t, s, "evidence_receipts") != 0 {
		t.Fatal("receipt remains after deletion")
	}
	r := request(s, "POST", "/v1/connections/local/devices/DEMO/evidence/ack", `{"params":{"eventId":"42","kind":"SNAPSHOT","packageSha256":"`+hash+`"}}`)
	if r.Code != 400 {
		t.Fatal("ACK accepted after retention", r.Code, r.Body)
	}
}
func TestOrdinaryJPEGRetentionKeepsPartialPackages(t *testing.T) {
	s := testService(t)
	image := []byte{1, 8, 0, 0, 0, 0, 0, 1, 0xff, 0xd8, 0xff, 0xd9}
	s.Ingest("local", "DEMO", "vdm/DEMO/image", image)
	if e := s.ProcessOne(context.Background()); e != nil {
		t.Fatal(e)
	}
	root := s.evidenceDir("local", "DEMO")
	matches, e := filepath.Glob(filepath.Join(root, "jpeg", "*.jpg"))
	if e != nil || len(matches) != 1 {
		t.Fatal(matches, e)
	}
	partial := filepath.Join(root, "retained.tar.part")
	if e = os.WriteFile(partial, []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	old := time.Now().Add(-8 * 24 * time.Hour)
	for _, file := range []string{matches[0], partial} {
		if e = os.Chtimes(file, old, old); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Prune(); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(matches[0]); !os.IsNotExist(e) {
		t.Fatal("expired JPEG not pruned", e)
	}
	if _, e = os.Stat(partial); e != nil {
		t.Fatal("partial package pruned", e)
	}
}

func TestLifecycleRetentionKeepsActiveAndAmbiguousIncidents(t *testing.T) {
	s := testService(t)
	old := time.Now().Add(-8 * 24 * time.Hour).Unix()
	cases := []struct {
		id, transition string
		dirty          int
		active         *bool
		reconciled     int64
		keep           bool
	}{
		{id: "terminal", transition: "RECOVERED", keep: false},
		{id: "cancelled", transition: "CANCELLED", keep: false},
		{id: "active", transition: "TRIGGERED", keep: true},
		{id: "ambiguous", transition: "CANCELLED", dirty: 1, keep: true},
		{id: "closed_snapshot", transition: "TRIGGERED", active: boolPointer(false), reconciled: old, keep: false},
		{id: "recent_snapshot", transition: "TRIGGERED", active: boolPointer(false), reconciled: time.Now().Unix(), keep: true},
		{id: "active_snapshot", transition: "RECOVERED", active: boolPointer(true), reconciled: old, keep: true},
	}
	for _, c := range cases {
		p := map[string]any{"alarmId": c.id, "transition": c.transition, "ts": old}
		if c.active != nil {
			p["currentActive"] = *c.active
			p["reconciledAt"] = c.reconciled
		}
		raw, _ := json.Marshal(p)
		if _, e := s.db.Exec("INSERT INTO alarms(connection,device,alarm_id,ts,payload,needs_reconcile) VALUES('local','DEMO',?,?,?,?)", c.id, old, string(raw), c.dirty); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.Prune(); e != nil {
		t.Fatal(e)
	}
	for _, c := range cases {
		var count int
		if e := s.db.QueryRow("SELECT count(*) FROM alarms WHERE alarm_id=?", c.id).Scan(&count); e != nil {
			t.Fatal(e)
		}
		if (count == 1) != c.keep {
			t.Errorf("%s: retained=%v want %v", c.id, count == 1, c.keep)
		}
	}
}
func boolPointer(value bool) *bool { return &value }
