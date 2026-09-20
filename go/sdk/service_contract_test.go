package sdk

import (
	"errors"
	"testing"
)

type durableMessage struct {
	acked   bool
	topic   string
	payload []byte
}

func (m *durableMessage) Duplicate() bool   { return false }
func (m *durableMessage) Qos() byte         { return 1 }
func (m *durableMessage) Retained() bool    { return false }
func (m *durableMessage) Topic() string     { return m.topic }
func (m *durableMessage) MessageID() uint16 { return 1 }
func (m *durableMessage) Payload() []byte   { return m.payload }
func (m *durableMessage) Ack()              { m.acked = true }
func TestManualACKRequiresDurableInsert(t *testing.T) {
	topics, _ := TopicsForDevice("D")
	m := &durableMessage{topic: "vdm/D/telemetry", payload: []byte(`{"x":1}`)}
	c := &Client{config: Config{Topics: topics}, codec: Codec{Format: JSON}}
	c.config.DurableHandler = func(string, []byte) error {
		if m.acked {
			t.Fatal("ack before persistence")
		}
		return errors.New("full")
	}
	c.onMessage(nil, m)
	if m.acked {
		t.Fatal("failed insert acked")
	}
	c.config.DurableHandler = func(string, []byte) error { return nil }
	c.onMessage(nil, m)
	if !m.acked {
		t.Fatal("persisted payload not acked")
	}
}
func TestNewPublicMethods(t *testing.T) {
	for _, format := range []PayloadFormat{JSON, Protobuf} {
		for _, method := range []string{"listAlarmEvents", "syncTelemetry", "getSyncStatus", "cancelSync"} {
			if _, _, err := (Codec{Format: format}).EncodeRPC(method, map[string]any{}, 1); err != nil {
				t.Fatal(method, err)
			}
		}
	}
}
