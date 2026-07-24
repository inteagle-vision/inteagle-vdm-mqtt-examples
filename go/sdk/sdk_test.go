package sdk

import (
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

func TestDecodeChecksSchemaVersion(t *testing.T) {
	topics, _ := TopicsForDevice("DEMO001")
	codec := Codec{Format: Protobuf}
	payload, _ := proto.Marshal(&vdmmqttv1.Attributes{SchemaVersion: 2})
	if _, err := codec.Decode(topics.Topic("attributes"), topics, payload); err == nil {
		t.Fatal("schema version 2 must be rejected")
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
