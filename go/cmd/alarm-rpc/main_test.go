package main

import (
	"strings"
	"testing"

	vdmmqttv1 "github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/generated"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"google.golang.org/protobuf/proto"
)

func TestRequestFilePreservesUint64BeforeEncoding(t *testing.T) {
	for _, id := range []string{"9007199254740993", "18446744073709551615"} {
		var request struct {
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := decodeRequestJSON([]byte(`{"method":"getEvidenceStatus","params":{"eventId":`+id+`}}`), &request); err != nil {
			t.Fatal(err)
		}
		for _, format := range []sdk.PayloadFormat{sdk.JSON, sdk.Protobuf} {
			raw, _, err := (sdk.Codec{Format: format}).EncodeRPC(request.Method, request.Params, 1)
			if err != nil {
				t.Fatal(err)
			}
			if format == sdk.JSON {
				if !strings.Contains(string(raw), `"eventId":`+id) {
					t.Fatalf("request ID was rounded: %s", raw)
				}
			} else {
				var decoded vdmmqttv1.RpcRequest
				if err := proto.Unmarshal(raw, &decoded); err != nil {
					t.Fatal(err)
				}
				if got := decoded.GetGetEvidenceStatus().GetEventId(); (id == "9007199254740993" && got != 9007199254740993) || (id == "18446744073709551615" && got != ^uint64(0)) {
					t.Fatalf("request ID was rounded: %d", got)
				}
			}
		}
	}
}

func TestRequestFileRejectsTrailingJSON(t *testing.T) {
	var request map[string]any
	if err := decodeRequestJSON([]byte(`{} {}`), &request); err == nil {
		t.Fatal("accepted multiple JSON requests")
	}
}
