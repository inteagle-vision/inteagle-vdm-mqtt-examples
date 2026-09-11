// Receive real device telemetry and attributes until Ctrl-C. No RPC is sent by default.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/internal/exampleconfig"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

func printMessage(output io.Writer, label string, message *sdk.DecodedPayload) error {
	fields, err := message.AsMap()
	if err != nil {
		return err
	}
	if label == "RESPONSE" {
		// Call(..., allowError=false) has already checked the successful RPC code.
		// Protobuf JSON omits zero scalars unless we explicitly show it here.
		fields["code"] = 0
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, label, string(raw))
	return err
}

type rpcCaller interface {
	Call(context.Context, string, any, int32, bool) (*sdk.DecodedPayload, error)
}

func queryAttributes(ctx context.Context, client rpcCaller, output io.Writer) error {
	response, err := client.Call(ctx, "getAttr", map[string]any{
		"keys": []string{"deviceId", "deviceModel", "fwVer", "measureStatus"},
	}, 0, false)
	if err != nil {
		return fmt.Errorf("QUERY_FAILED: %w", err)
	}
	return printMessage(output, "RESPONSE", response)
}

func subscriptions(query bool) []string {
	result := []string{"telemetry", "attributes"}
	if query {
		result = append(result, "rpc/resp")
	}
	return result
}

func run() error {
	query := flag.Bool("query-attributes", false, "Query device attributes once, then continue receiving")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	config, err := exampleconfig.Load(subscriptions(*query)...)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	messages := make(chan *sdk.DecodedPayload, 64)
	errors := make(chan error, 16)
	report := func(err error) {
		select {
		case errors <- err:
		default:
		}
	}
	client, err := sdk.NewClient(config, func(message *sdk.DecodedPayload) {
		if message.Suffix != "telemetry" && message.Suffix != "attributes" {
			return
		}
		select {
		case messages <- message:
		default:
			report(fmt.Errorf("data queue full; output cannot keep up"))
		}
	}, report)
	if err != nil {
		return err
	}
	defer client.Close()
	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = client.Start(startCtx)
	cancel()
	if err != nil {
		return err
	}
	fmt.Printf("READY receive-data deviceId=%s format=%s\n", os.Getenv("VDM_DEVICE_ID"), config.PayloadFormat)
	if *query {
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		queryErr := queryAttributes(queryCtx, client, os.Stdout)
		cancel()
		if queryErr != nil {
			return queryErr
		}

	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-messages:
			if err := printMessage(os.Stdout, message.Suffix, message); err != nil {
				return err
			}
		case err := <-errors:
			fmt.Fprintln(os.Stderr, "ERROR", err)
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR", err)
		os.Exit(1)
	}
}
