package sdk

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestSharedAll33RPCWireFixtures(t *testing.T) {
	raw, e := os.ReadFile("../../tests/fixtures/rpc-cases.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct {
		Method   string         `json:"method"`
		JSON     map[string]any `json:"json"`
		Protobuf map[string]any `json:"protobuf"`
		ReqID    int32          `json:"reqId"`
		Hex      string         `json:"protobufHex"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e = d.Decode(&fixtures); e != nil {
		t.Fatal(e)
	}
	if len(fixtures) != 33 {
		t.Fatal(len(fixtures))
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Method, func(t *testing.T) {
			encoded, _, e := (Codec{Format: Protobuf}).EncodeRPC(fixture.Method, fixture.Protobuf, fixture.ReqID)
			if e != nil {
				t.Fatal(e)
			}
			if hex.EncodeToString(encoded) != fixture.Hex {
				t.Fatalf("protobuf wire mismatch: got %x want %s", encoded, fixture.Hex)
			}
			encoded, _, e = (Codec{Format: JSON}).EncodeRPC(fixture.Method, fixture.JSON, fixture.ReqID)
			if e != nil {
				t.Fatal(e)
			}
			var got struct {
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
				ReqID  int32          `json:"reqId"`
			}
			d := json.NewDecoder(bytes.NewReader(encoded))
			d.UseNumber()
			if e = d.Decode(&got); e != nil {
				t.Fatal(e)
			}
			if got.Method != fixture.Method || got.ReqID != fixture.ReqID || !reflect.DeepEqual(got.Params, fixture.JSON) {
				t.Fatalf("JSON fields changed: %s", encoded)
			}
		})
	}
}
