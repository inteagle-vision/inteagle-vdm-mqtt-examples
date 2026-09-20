// Package service implements the durable VDM business service. HTTP and MQTT
// callbacks delegate to domain handlers; only workers perform outgoing RPC.
package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type RPCClient interface {
	Call(context.Context, string, any, int32, bool) (*sdk.DecodedPayload, error)
	Connected() bool
}
type Device struct {
	ConnectionID string   `json:"connectionId"`
	DeviceID     string   `json:"deviceId"`
	Format       string   `json:"format"`
	Capabilities []string `json:"capabilities"`
}
type Service struct {
	config                            Config
	dir, token, webhook, webhookToken string
	db                                *sql.DB
	lock                              *os.File
	clients                           map[string]RPCClient
	devices                           map[string]Device
	httpClient                        *http.Client
	stop                              context.CancelFunc
	wg                                sync.WaitGroup
	started                           bool
	mu                                sync.Mutex
	ingestFailures                    atomic.Int64
	evidenceMu                        sync.Mutex
	rpcSlots                          chan struct{}
}

func New(config Config, dir, token, webhook, webhookToken string) (*Service, error) {
	if e := config.Validate(); e != nil {
		return nil, e
	}
	if webhook != "" {
		u, e := url.Parse(webhook)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			return nil, errors.New("invalid webhook URL")
		}
	}
	db, lock, e := openStore(dir)
	if e != nil {
		return nil, e
	}
	s := &Service{config: config, dir: dir, token: token, webhook: webhook, webhookToken: webhookToken, db: db, lock: lock, clients: map[string]RPCClient{}, devices: map[string]Device{}, httpClient: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, rpcSlots: make(chan struct{}, 32)}
	for _, c := range config.Connections {
		for _, d := range c.Devices {
			caps := d.Capabilities
			if caps == nil {
				caps = []string{}
			}
			s.devices[c.ID+"/"+d.ID] = Device{c.ID, d.ID, d.Format, caps}
		}
	}
	return s, nil
}
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("service already started")
	}
	ctx, s.stop = context.WithCancel(ctx)
	for _, conn := range s.config.Connections {
		for _, device := range conn.Devices {
			c, d := conn, device
			topics, _ := sdk.TopicsForDevice(d.ID)
			sum := sha256.Sum256([]byte("go/" + c.ID + "/" + d.ID))
			client, e := sdk.NewClient(sdk.Config{Host: c.Host, Port: c.Port, Topics: topics, PayloadFormat: sdk.PayloadFormat(d.Format), Username: os.Getenv(c.UsernameEnv), Password: os.Getenv(c.PasswordEnv), ClientID: "vdm-go-" + hex.EncodeToString(sum[:16]), QoS: 1, PersistentSession: true, ConnectTimeout: 5 * time.Second, SubscriptionSuffixes: []string{"telemetry", "attributes", "3A", "event", "image", "rpc/resp"}, DurableHandler: func(topic string, p []byte) error { return s.Ingest(c.ID, d.ID, topic, p) }}, nil, func(error) { log.Printf("MQTT connection degraded: %s/%s", c.ID, d.ID) })
			if e != nil {
				s.stop()
				return e
			}
			s.clients[c.ID+"/"+d.ID] = client
		}
	}
	// Build the complete registry before launching workers; map reads then need no locks.
	for key, transport := range s.clients {
		client, ok := transport.(*sdk.Client)
		if !ok {
			continue
		}
		s.wg.Add(1)
		go func(key string, c *sdk.Client) {
			defer s.wg.Done()
			defer c.Close()
			for ctx.Err() == nil {
				if !c.Connected() {
					if e := c.Start(ctx); e != nil && ctx.Err() == nil {
						log.Printf("MQTT unavailable: %s", key)
					}
				}
				if !pause(ctx, 2*time.Second) {
					return
				}
			}
		}(key, client)
	}
	s.started = true
	for _, worker := range []func(context.Context) error{s.ProcessOne, s.DeliverOne, s.RunJobOne} {
		w := worker
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for ctx.Err() == nil {
				if e := w(ctx); e != nil && !errors.Is(e, sql.ErrNoRows) && ctx.Err() == nil {
					log.Printf("durable worker: %v", e)
				}
				if !pause(ctx, 100*time.Millisecond) {
					return
				}
			}
		}()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for pause(ctx, time.Hour) {
			if e := s.Prune(); e != nil {
				log.Printf("retention: %v", e)
			}
		}
	}()
	return nil
}
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (s *Service) Close() error {
	if s.stop != nil {
		s.stop()
	}
	s.wg.Wait()
	e := s.db.Close()
	s.lock.Close()
	return e
}
func (s *Service) Ingest(connection, device, topic string, payload []byte) (err error) {
	defer func() {
		if err != nil {
			s.ingestFailures.Add(1)
		}
	}()
	if _, ok := s.devices[connection+"/"+device]; !ok {
		return errors.New("unknown source")
	}
	limit := int64(1 << 20)
	if topic == "vdm/"+device+"/image" {
		limit = min(s.config.MaxEvidenceBytes, int64(32<<20))
	}
	if int64(len(payload)) > limit {
		return ErrOverloaded
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var n int
	if e = tx.QueryRow("SELECT count(*) FROM inbox WHERE status IN ('pending','failed')").Scan(&n); e != nil {
		return e
	}
	if n >= s.config.MaxInboxRows {
		return ErrOverloaded
	}
	_, e = tx.Exec("INSERT INTO inbox(connection,device,topic,payload,created) VALUES(?,?,?,?,?)", connection, device, topic, payload, time.Now().Unix())
	if e != nil {
		return e
	}
	return tx.Commit()
}

type inboxItem struct {
	id                        int64
	connection, device, topic string
	payload                   []byte
	attempts                  int
}

func (s *Service) ProcessOne(ctx context.Context) error {
	var row inboxItem
	e := s.db.QueryRowContext(ctx, "SELECT id,connection,device,topic,payload,attempts FROM inbox WHERE status='pending' AND next_at<=? ORDER BY id LIMIT 1", time.Now().Unix()).Scan(&row.id, &row.connection, &row.device, &row.topic, &row.payload, &row.attempts)
	if e != nil {
		return e
	}
	d, ok := s.devices[row.connection+"/"+row.device]
	if !ok {
		return s.inboxFailed(row, errors.New("device removed from configuration"))
	}
	topics, _ := sdk.TopicsForDevice(row.device)
	decoded, e := (sdk.Codec{Format: sdk.PayloadFormat(d.Format)}).Decode(row.topic, topics, row.payload)
	if e != nil {
		return s.inboxFailed(row, e)
	}
	if decoded.Suffix == "image" {
		e = s.processImage(ctx, row, decoded)
	} else {
		var fields map[string]any
		fields, e = decoded.AsMap()
		if e == nil {
			e = s.processObject(ctx, row, decoded.Suffix, fields)
		}
	}
	if e != nil {
		return s.inboxFailed(row, e)
	}
	return nil
}
func (s *Service) inboxFailed(row inboxItem, cause error) error {
	attempts := row.attempts + 1
	status := "pending"
	if attempts >= 8 {
		status = "failed"
	}
	_, e := s.db.Exec("UPDATE inbox SET attempts=?,status=?,next_at=?,error=? WHERE id=?", attempts, status, time.Now().Unix()+int64(1<<min(attempts, 8)), cause.Error(), row.id)
	if e != nil {
		return e
	}
	return cause
}
func (s *Service) processObject(ctx context.Context, row inboxItem, suffix string, fields map[string]any) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	payload, e := json.Marshal(fields)
	if e != nil {
		return e
	}
	switch suffix {
	case "telemetry", "attributes":
		e = s.storeLatest(tx, row, suffix, string(payload))
	case "3A":
		e = s.storeAlarm(tx, row, fields)
	case "event":
		sum := sha256.Sum256(payload)
		_, e = tx.Exec("INSERT OR IGNORE INTO evidence_events(key,connection,device,payload,created) VALUES(?,?,?,?,?)", stableKey(row.connection, row.device, hex.EncodeToString(sum[:]), "evidence"), row.connection, row.device, string(payload), time.Now().Unix())
	default:
		e = fmt.Errorf("unsupported business topic %q", suffix)
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec("UPDATE inbox SET status='done',error=NULL WHERE id=?", row.id); e != nil {
		return e
	}
	return tx.Commit()
}
func stableKey(connection, device, event, transition string) string {
	b, _ := json.Marshal([]string{connection, device, event, transition})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (s *Service) Prune() error {
	cutoff := time.Now().Add(-time.Duration(s.config.RetentionDays) * 24 * time.Hour).Unix()
	if e := s.pruneEvidence(cutoff); e != nil {
		return e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, q := range []string{"DELETE FROM inbox WHERE status='done' AND created<?", "DELETE FROM alarm_events WHERE created<?", "DELETE FROM evidence_events WHERE created<?", "DELETE FROM outbox WHERE status='done' AND created<?", "DELETE FROM jobs WHERE status='done' AND created<? AND (kind!='evidence_ack' OR NOT EXISTS(SELECT 1 FROM evidence_receipts r WHERE r.connection=jobs.connection AND r.device=jobs.device AND r.event_id=json_extract(jobs.params,'$.eventId') AND r.hash=json_extract(jobs.params,'$.packageSha256')))"} {
		if _, e = tx.Exec(q, cutoff); e != nil {
			return e
		}
	}
	// An authoritative snapshot takes precedence over the historical event.
	// Unknown/currently active and unresolved lifecycles must remain available.
	if _, e = tx.Exec(`DELETE FROM alarms WHERE ts<? AND needs_reconcile=0 AND (
  (json_type(payload,'$.currentActive') IS NULL AND json_extract(payload,'$.transition') IN ('RECOVERED','CANCELLED'))
  OR (json_extract(payload,'$.currentActive')=0 AND CAST(json_extract(payload,'$.reconciledAt') AS INTEGER)<?)
 )`, cutoff, cutoff); e != nil {
		return e
	}
	for _, table := range []string{"alarm_events", "evidence_events"} {
		if _, e = tx.Exec("DELETE FROM "+table+" WHERE rowid IN (SELECT rowid FROM "+table+" ORDER BY created,rowid LIMIT max(0,(SELECT count(*) FROM "+table+")-?))", s.config.MaxInboxRows*10); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Service) evidenceDir(connection, device string) string {
	return filepath.Join(s.dir, "evidence", connection, device)
}
