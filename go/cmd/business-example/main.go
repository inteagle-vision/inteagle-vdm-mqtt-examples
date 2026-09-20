// Concrete examples for each business domain. Printing is the default; --execute
// performs the selected RPC against the explicit MQTT_HOST/VDM_DEVICE_ID.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/internal/businessexamples"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/internal/exampleconfig"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"os"
	"time"
)

func run() error {
	name := flag.String("case", "device", "device, targets, measurement, measurement-status, measurement-cancel, alarms, evidence")
	execute := flag.Bool("execute", false, "Send RPC; targets and measurement examples change device state")
	flag.Parse()
	format, e := sdk.ParsePayloadFormat(exampleconfig.Env("VDM_PAYLOAD_FORMAT", "protobuf"))
	if e != nil {
		return e
	}
	request, e := businessexamples.Select(*name, format)
	if e != nil {
		return e
	}
	if _, _, e = (sdk.Codec{Format: format}).EncodeRPC(request.Method, request.Params, 1); e != nil {
		return e
	}
	if !*execute {
		return json.NewEncoder(os.Stdout).Encode(request)
	}
	config, e := exampleconfig.Load("rpc/resp")
	if e != nil {
		return e
	}
	client, e := sdk.NewClient(config, nil, func(error) { fmt.Fprintln(os.Stderr, "MQTT connection error") })
	if e != nil {
		return e
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e = client.Start(ctx); e != nil {
		return e
	}
	response, e := client.Call(ctx, request.Method, request.Params, 0, false)
	if e != nil {
		return e
	}
	fields, e := response.AsMap()
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(fields)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
