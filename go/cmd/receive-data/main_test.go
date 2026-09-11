package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/generated"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"reflect"
	"strings"
	"testing"
)

func TestDataOnlySubscriptionsDoNotIncludeRPCOrImages(t *testing.T) {
	if got := subscriptions(false); !reflect.DeepEqual(got, []string{"telemetry", "attributes"}) {
		t.Fatal(got)
	}
	if got := subscriptions(true); !reflect.DeepEqual(got, []string{"telemetry", "attributes", "rpc/resp"}) {
		t.Fatal(got)
	}
}

type fakeRPC struct {
	response *sdk.DecodedPayload
	err      error
}

func (f fakeRPC) Call(_ context.Context, method string, _ any, _ int32, allowError bool) (*sdk.DecodedPayload, error) {
	if method != "getAttr" || allowError {
		panic("unexpected query contract")
	}
	return f.response, f.err
}

func TestDataAndSuccessfulQueryOutputMatchDocumentedPrefixes(t *testing.T) {
	var output bytes.Buffer
	data := &sdk.DecodedPayload{Topic: "vdm/device1/telemetry", Suffix: "telemetry", Value: map[string]any{"disp": map[string]any{}}}
	if err := printMessage(&output, data.Suffix, data); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "telemetry {") || strings.Contains(output.String(), "vdm/device1") {
		t.Fatal(output.String())
	}
	for _, value := range []any{map[string]any{"reqId": 1}, &pb.RpcResponse{SchemaVersion: 1, ReqId: 1}} {
		output.Reset()
		if err := queryAttributes(context.Background(), fakeRPC{response: &sdk.DecodedPayload{Value: value}}, &output); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(output.String(), "RESPONSE {") {
			t.Fatal(output.String())
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(output.String(), "RESPONSE ")), &fields); err != nil {
			t.Fatal(err)
		}
		if code, ok := fields["code"]; !ok || code != float64(0) {
			t.Fatal(fields)
		}
	}
}

func TestQueryFailureReturnsErrorAndDoesNotPrintSuccessfulResponse(t *testing.T) {
	var output bytes.Buffer
	if err := queryAttributes(context.Background(), fakeRPC{err: fmt.Errorf("RPC rejected")}, &output); err == nil {
		t.Fatal("query error was swallowed")
	}
	if output.Len() != 0 {
		t.Fatal(output.String())
	}
}
