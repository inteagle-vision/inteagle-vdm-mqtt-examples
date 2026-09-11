package sdk

import (
	"encoding/json"
	vdmmqttv1 "github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/generated"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

func alarmExamples(t *testing.T, name string) []map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile("../../examples/alarms/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func TestAlarmConfigurationExamples(t *testing.T) {
	for _, item := range alarmExamples(t, "requests") {
		var method string
		if err := json.Unmarshal(item["method"], &method); err != nil {
			t.Fatal(err)
		}
		for _, format := range []PayloadFormat{Protobuf, JSON} {
			var params map[string]any
			if err := json.Unmarshal(item[string(format)], &params); err != nil {
				t.Fatal(err)
			}
			raw, _, err := (Codec{Format: format}).EncodeRPC(method, params, 91)
			if err != nil {
				t.Fatalf("%s: %v", item["name"], err)
			}
			if format == Protobuf {
				wantBytes, err := json.Marshal(map[string]any{"schemaVersion": 1, "reqId": 91, method: params})
				if err != nil {
					t.Fatal(err)
				}
				want, got := &vdmmqttv1.RpcRequest{}, &vdmmqttv1.RpcRequest{}
				if err := protojson.Unmarshal(wantBytes, want); err != nil {
					t.Fatal(err)
				}
				if err := proto.Unmarshal(raw, got); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(want, got) {
					t.Fatalf("wrong %s request: %v", method, got)
				}
			} else {
				var got map[string]json.RawMessage
				if err := json.Unmarshal(raw, &got); err != nil {
					t.Fatal(err)
				}
				var actual map[string]any
				if err := json.Unmarshal(got["params"], &actual); err != nil {
					t.Fatal(err)
				}
				a, _ := json.Marshal(actual)
				b, _ := json.Marshal(params)
				if string(a) != string(b) {
					t.Fatalf("JSON params changed: %s", raw)
				}
			}
		}
	}
}

func TestAlarmLifecycleExamples(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	for _, item := range alarmExamples(t, "events") {
		for _, format := range []PayloadFormat{Protobuf, JSON} {
			raw := []byte(item[string(format)])
			var expected map[string]any
			if err := json.Unmarshal(raw, &expected); err != nil {
				t.Fatal(err)
			}
			if format == Protobuf {
				message := &vdmmqttv1.Alarm{}
				if err := protojson.Unmarshal(raw, message); err != nil {
					t.Fatal(err)
				}
				var err error
				raw, err = proto.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
			}
			decoded, err := (Codec{Format: format}).Decode(topics.Topic("3A"), topics, raw)
			if err != nil {
				t.Fatal(err)
			}
			fields, err := decoded.AsMap()
			if err != nil {
				t.Fatal(err)
			}
			_, hasLevel := fields["level"]
			_, wantLevel := expected["level"]
			if fields["eventId"] != expected["eventId"] || fields["alarmId"] != expected["alarmId"] || hasLevel != wantLevel {
				t.Fatalf("lost ID precision or optional level: %v", fields)
			}
		}
	}
}
