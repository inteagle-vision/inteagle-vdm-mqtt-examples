// Run one MQTT alarm RPC. No arguments queries capabilities without writing.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Preserve decimal integers when callers load a Protobuf request from JSON.
// json.Unmarshal into map[string]any would first round uint64 values to float64.
func decodeRequestJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func run() error {
	name := flag.String("case", "capabilities", "Name in examples/alarms/requests.json")
	file := flag.String("request", "", "Your complete {method,params} JSON file for the selected format")
	printOnly := flag.Bool("print-only", false, "Validate encoding without connecting")
	seconds := flag.Int("listen", 0, "Seconds to receive 3A/event after RPC")
	flag.Parse()
	if *seconds < 0 {
		return fmt.Errorf("--listen must be nonnegative")
	}
	selectedCase := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "case" {
			selectedCase = true
		}
	})
	if selectedCase && *file != "" {
		return fmt.Errorf("choose --case or --request")
	}
	format, err := sdk.ParsePayloadFormat(env("VDM_PAYLOAD_FORMAT", "protobuf"))
	if err != nil {
		return err
	}
	var request struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if *file != "" {
		raw, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		if err = decodeRequestJSON(raw, &request); err != nil {
			return err
		}
	} else {
		raw, err := os.ReadFile("../examples/alarms/requests.json")
		if err != nil {
			return err
		}
		var cases []struct {
			Name     string         `json:"name"`
			Method   string         `json:"method"`
			JSON     map[string]any `json:"json"`
			Protobuf map[string]any `json:"protobuf"`
		}
		if err = decodeRequestJSON(raw, &cases); err != nil {
			return err
		}
		for _, item := range cases {
			if item.Name == *name {
				request.Method = item.Method
				request.Params = item.JSON
				if format == sdk.Protobuf {
					request.Params = item.Protobuf
				}
				break
			}
		}
		if request.Method == "" {
			return fmt.Errorf("unknown case %s", *name)
		}
	}
	reqID := rand.Int32N(2147483646) + 1
	raw, _, err := (sdk.Codec{Format: format}).EncodeRPC(request.Method, request.Params, reqID)
	if err != nil {
		return err
	}
	display, _ := json.MarshalIndent(request, "", "  ")
	fmt.Println(string(display))
	fmt.Printf("format=%s bytes=%d reqId=%d\n", format, len(raw), reqID)
	if *printOnly {
		return nil
	}
	host, device := os.Getenv("MQTT_HOST"), os.Getenv("VDM_DEVICE_ID")
	if host == "" || device == "" {
		return fmt.Errorf("set MQTT_HOST and VDM_DEVICE_ID")
	}
	port, err := strconv.Atoi(env("MQTT_PORT", "1883"))
	if err != nil {
		return err
	}
	topics, err := sdk.TopicsForDevice(device)
	if err != nil {
		return err
	}
	client, err := sdk.NewClient(sdk.Config{Host: host, Port: port, Topics: topics, PayloadFormat: format, QoS: 1, Username: os.Getenv("MQTT_USERNAME"), Password: os.Getenv("MQTT_PASSWORD")}, func(message *sdk.DecodedPayload) {
		if message.Suffix == "3A" || message.Suffix == "event" {
			fields, err := message.AsMap()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			raw, _ := json.Marshal(fields)
			fmt.Println(message.Suffix, string(raw))
		}
	}, func(err error) { fmt.Fprintln(os.Stderr, "DECODE_ERROR", err) })
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = client.Start(ctx); err != nil {
		return err
	}
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer rpcCancel()
	response, err := client.Call(rpcCtx, request.Method, request.Params, reqID, false)
	if err != nil {
		return err
	}
	fields, err := response.AsMap()
	if err != nil {
		return err
	}
	display, err = json.Marshal(fields)
	if err != nil {
		return err
	}
	fmt.Println("RESPONSE", string(display))
	time.Sleep(time.Duration(*seconds) * time.Second)
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
