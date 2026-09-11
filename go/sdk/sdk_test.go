package sdk

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	vdmmqttv1 "github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/generated"
	"google.golang.org/protobuf/proto"
)

func TestProtobufRPCUsesTypedOneof(t *testing.T) {
	codec := Codec{Format: Protobuf}
	payload, expected, err := codec.EncodeRPC("getAttr", map[string]any{"keys": []string{"deviceId"}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	request := &vdmmqttv1.RpcRequest{}
	if err := proto.Unmarshal(payload, request); err != nil {
		t.Fatal(err)
	}
	if request.GetSchemaVersion() != 1 || request.GetReqId() != 7 || request.GetGetAttr() == nil {
		t.Fatalf("unexpected request: %v", request)
	}
	if expected != "get_attr" || len(request.GetGetAttr().GetKeys()) != 1 {
		t.Fatalf("unexpected method mapping: expected=%s request=%v", expected, request)
	}
}

func TestInternalRPCIsRejected(t *testing.T) {
	codec := Codec{Format: Protobuf}
	if _, _, err := codec.EncodeRPC("privateDeviceCommand", map[string]any{}, 8); err == nil {
		t.Fatal("internal method must be rejected")
	}
}

func TestRPCErrorTextIsDerivedLocallyFromNumericCode(t *testing.T) {
	_, _, message, _, err := responseInfo(map[string]any{
		"reqId": float64(8), "code": float64(4), "msg": "private device diagnostic",
	})
	if err != nil {
		t.Fatal(err)
	}
	if message != "RPC request rate limited" {
		t.Fatalf("unexpected local message: %s", message)
	}

	_, _, message, _, err = responseInfo(&vdmmqttv1.RpcResponse{
		SchemaVersion: 1, ReqId: 9, Code: 300, Message: "private device diagnostic",
	})
	if err != nil {
		t.Fatal(err)
	}
	if message != "motor unavailable" {
		t.Fatalf("unexpected local message: %s", message)
	}
}

func TestAllPublicRPCMethodsBuildTypedBody(t *testing.T) {
	if len(publicRPCFields) != 29 {
		t.Fatalf("expected 29 public methods, got %d", len(publicRPCFields))
	}
	codec := Codec{Format: Protobuf}
	reqID := int32(100)
	for method, expected := range publicRPCFields {
		t.Run(method, func(t *testing.T) {
			payload, actual, err := codec.EncodeRPC(method, map[string]any{}, reqID)
			if err != nil {
				t.Fatal(err)
			}
			request := &vdmmqttv1.RpcRequest{}
			if err := proto.Unmarshal(payload, request); err != nil {
				t.Fatal(err)
			}
			selected := request.ProtoReflect().WhichOneof(
				request.ProtoReflect().Descriptor().Oneofs().ByName("request"),
			)
			if selected == nil || string(selected.Name()) != expected || actual != expected {
				t.Fatalf("expected=%s actual=%s request=%v", expected, actual, request)
			}
		})
		reqID++
	}
}

func TestUnavailableRPCMethodsAreRejectedInBothFormats(t *testing.T) {
	unavailable := []string{
		"getStorageInfo", "queryTelemetry", "uploadS3", "setCruisePoint",
		"removeCruisePoint", "startPatrol", "stopPatrol", "getPatrolStatus",
	}
	for _, format := range []PayloadFormat{JSON, Protobuf} {
		codec := Codec{Format: format}
		for _, method := range unavailable {
			t.Run(string(format)+"/"+method, func(t *testing.T) {
				if _, _, err := codec.EncodeRPC(method, map[string]any{}, 200); err == nil {
					t.Fatalf("unavailable method %s must be rejected", method)
				}
			})
		}
	}
}

func TestDecodeEvidencePackageChunk(t *testing.T) {
	packageBytes := make([]byte, 512)
	copy(packageBytes[257:262], "ustar")
	payload := make([]byte, evidencePackageHeaderLength+len(packageBytes))
	payload[0], payload[1], payload[2], payload[3] = 2, evidencePackageHeaderLength, 1, 1
	binary.BigEndian.PutUint64(payload[4:12], 9001)
	binary.BigEndian.PutUint64(payload[12:20], uint64(len(packageBytes)))
	packageHash := sha256.Sum256(packageBytes)
	copy(payload[20:52], packageHash[:])
	binary.BigEndian.PutUint32(payload[52:56], 0)
	binary.BigEndian.PutUint32(payload[56:60], 1)
	binary.BigEndian.PutUint64(payload[60:68], 0)
	binary.BigEndian.PutUint32(payload[68:72], uint32(len(packageBytes)))
	copy(payload[76:], packageBytes)

	decoded, err := decodeImage(payload)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok := decoded.(*EvidencePackageChunk)
	if !ok || chunk.EventID != 9001 || string(chunk.Chunk) != string(packageBytes) {
		t.Fatalf("unexpected evidence chunk: %#v", decoded)
	}
}

func TestDecodeChecksSchemaVersion(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	codec := Codec{Format: Protobuf}
	payload, _ := proto.Marshal(&vdmmqttv1.Attributes{SchemaVersion: 2})
	if _, err := codec.Decode(topics.Topic("attributes"), topics, payload); err == nil {
		t.Fatal("schema version 2 must be rejected")
	}
}

func TestDecodeEvidenceAsTypedEvent(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	codec := Codec{Format: Protobuf}
	payload, _ := proto.Marshal(&vdmmqttv1.Event{
		SchemaVersion: 1,
		TimestampS:    1721805600,
		EventType:     vdmmqttv1.EventType_EVENT_TYPE_ALARM_EVIDENCE,
		Detail: &vdmmqttv1.Event_AlarmEvidence{AlarmEvidence: &vdmmqttv1.AlarmEvidenceEventDetail{
			EventId: 9001,
			Kind:    vdmmqttv1.EvidenceKind_EVIDENCE_KIND_SNAPSHOT,
			State:   vdmmqttv1.EvidenceState_EVIDENCE_STATE_READY,
		}},
	})
	decoded, err := codec.Decode(topics.Topic("event"), topics, payload)
	if err != nil {
		t.Fatal(err)
	}
	event, ok := decoded.Value.(*vdmmqttv1.Event)
	if !ok || event.GetAlarmEvidence().GetEventId() != 9001 {
		t.Fatalf("unexpected evidence: %#v", decoded.Value)
	}
}

func TestDecodeReturnsReadableFields(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	codec := Codec{Format: Protobuf}
	payload, _ := proto.Marshal(&vdmmqttv1.Attributes{
		SchemaVersion:   1,
		DeviceId:        proto.String("DEMO001"),
		FirmwareVersion: proto.String("example-1.0.0"),
	})
	decoded, err := codec.Decode(topics.Topic("attributes"), topics, payload)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := decoded.AsMap()
	if err != nil {
		t.Fatal(err)
	}
	if fields["deviceId"] != "DEMO001" || fields["firmwareVersion"] != "example-1.0.0" {
		t.Fatalf("unexpected readable fields: %#v", fields)
	}
}

func TestOptionalSubscriptionsKeepWildcardDefaultAndRejectForeignTopics(t *testing.T) {
	topics, _ := TopicsForDevice("device1")
	config := Config{Topics: topics, QoS: 1}
	actual, err := subscriptionTopics(config)
	if err != nil || len(actual) != 1 || actual["vdm/device1/#"] != 1 {
		t.Fatalf("default changed: %v %v", actual, err)
	}
	config.SubscriptionSuffixes = []string{"telemetry", "attributes", "telemetry"}
	actual, err = subscriptionTopics(config)
	if err != nil || len(actual) != 2 || actual["vdm/device1/telemetry"] != 1 || actual["vdm/device1/attributes"] != 1 {
		t.Fatalf("filtered: %v %v", actual, err)
	}
	for _, value := range []string{"#", "+", "vdm/other/image", "../image", ""} {
		config.SubscriptionSuffixes = []string{value}
		if _, err := subscriptionTopics(config); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestSubscriptionReadinessRejectsMissingAndFailedSUBACK(t *testing.T) {
	requested := map[string]byte{"vdm/device1/telemetry": 1, "vdm/device1/attributes": 1}
	if err := validateSubscriptionResults(requested, map[string]byte{"vdm/device1/telemetry": 1, "vdm/device1/attributes": 0}); err != nil {
		t.Fatal(err)
	}
	for _, granted := range []map[string]byte{
		{"vdm/device1/telemetry": 1, "vdm/device1/attributes": 128},
		{"vdm/device1/telemetry": 1},
	} {
		if err := validateSubscriptionResults(requested, granted); err == nil {
			t.Fatalf("invalid SUBACK accepted: %v", granted)
		}
	}
}
