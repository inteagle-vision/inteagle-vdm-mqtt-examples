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
	if _, _, err := codec.EncodeRPC("wySetAttributes", map[string]any{}, 8); err == nil {
		t.Fatal("internal method must be rejected")
	}
}

func TestAllPublicRPCMethodsBuildTypedBody(t *testing.T) {
	if len(publicRPCFields) != 32 {
		t.Fatalf("expected 32 public methods, got %d", len(publicRPCFields))
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

func TestDecodeEvidenceImageChunk(t *testing.T) {
	jpeg := []byte{0xff, 0xd8, 'v', 'd', 'm', 0xff, 0xd9}
	payload := make([]byte, evidenceImageHeaderLength+len(jpeg))
	payload[0], payload[1], payload[2], payload[3] = 2, evidenceImageHeaderLength, 1, 1
	binary.BigEndian.PutUint64(payload[4:12], 1721805600000)
	binary.BigEndian.PutUint64(payload[12:20], 9001)
	binary.BigEndian.PutUint16(payload[20:22], 0)
	binary.BigEndian.PutUint16(payload[22:24], 1)
	actualOffset := int32(-1000)
	binary.BigEndian.PutUint32(payload[24:28], uint32(actualOffset))
	binary.BigEndian.PutUint32(payload[28:32], uint32(len(jpeg)))
	jpegHash := sha256.Sum256(jpeg)
	copy(payload[32:64], jpegHash[:])
	manifestHash := sha256.Sum256([]byte("manifest"))
	copy(payload[64:96], manifestHash[:])
	binary.BigEndian.PutUint16(payload[96:98], 0)
	binary.BigEndian.PutUint16(payload[98:100], 1)
	binary.BigEndian.PutUint32(payload[100:104], 0)
	binary.BigEndian.PutUint32(payload[104:108], uint32(len(jpeg)))
	copy(payload[112:], jpeg)

	decoded, err := decodeImage(payload)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok := decoded.(*EvidenceImageChunk)
	if !ok || chunk.EventID != 9001 || chunk.ActualOffsetMS != -1000 || string(chunk.Chunk) != string(jpeg) {
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

func TestDecodeEvidenceWithoutSchemaVersion(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	codec := Codec{Format: Protobuf}
	payload, _ := proto.Marshal(&vdmmqttv1.AlarmEvidence{
		EventId: 9001,
		Kind:    vdmmqttv1.EvidenceKind_EVIDENCE_KIND_SNAPSHOT,
		State:   vdmmqttv1.EvidenceState_EVIDENCE_STATE_READY,
	})
	decoded, err := codec.Decode(topics.Topic("evidence"), topics, payload)
	if err != nil {
		t.Fatal(err)
	}
	if evidence, ok := decoded.Value.(*vdmmqttv1.AlarmEvidence); !ok || evidence.GetEventId() != 9001 {
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
