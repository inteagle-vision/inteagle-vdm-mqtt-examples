// VDM MQTT SDK 的 Go 订阅解析示例。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

var requiredSuffixes = map[string]struct{}{
	"telemetry": {}, "attributes": {}, "event": {}, "3A": {},
	"rpc/req": {}, "rpc/resp": {}, "image": {},
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL consumer=go reason=%v\n", err)
		os.Exit(1)
	}
}

func run() error {
	port, err := strconv.Atoi(env("MQTT_PORT", "1883"))
	if err != nil {
		return fmt.Errorf("MQTT_PORT 无效: %w", err)
	}
	timeoutSeconds, err := strconv.ParseFloat(env("VDM_WAIT_TIMEOUT", "45"), 64)
	if err != nil || timeoutSeconds <= 0 {
		return errors.New("VDM_WAIT_TIMEOUT 必须是正数")
	}
	format, err := sdk.ParsePayloadFormat(env("VDM_PAYLOAD_FORMAT", "protobuf"))
	if err != nil {
		return err
	}
	topics, err := sdk.NewTopics(env("VDM_BASE_TOPIC", "vdm/DEMO001"))
	if err != nil {
		return err
	}
	runID := env("VDM_RUN_ID", "manual")
	name := env("VDM_CONSUMER_NAME", "go")
	controlTopic := func(kind string) string {
		return fmt.Sprintf("vdm-example/%s/%s/%s", runID, kind, name)
	}

	var client *sdk.Client
	var mu sync.Mutex
	seen := make(map[string]struct{})
	resultSent := false
	rpcStarted := false
	rpcPassed := false
	publishResult := func(value string) {
		if client != nil {
			_ = client.PublishRaw(controlTopic("result"), value, true)
		}
	}
	fail := func(reason string) {
		mu.Lock()
		if resultSent {
			mu.Unlock()
			return
		}
		resultSent = true
		mu.Unlock()
		fmt.Printf("FAIL consumer=%s reason=%s\n", name, reason)
		publishResult("FAIL:" + reason)
	}
	completeIfReady := func() {
		values := []string(nil)
		mu.Lock()
		if !resultSent && rpcPassed && len(seen) == len(requiredSuffixes) {
			resultSent = true
			values = make([]string, 0, len(seen))
			for suffix := range seen {
				values = append(values, suffix)
			}
		}
		mu.Unlock()
		if values != nil {
			sort.Strings(values)
			publishResult("PASS")
			fmt.Printf("PASS consumer=%s profile=%s topics=%s rpc=getAttr\n",
				name, format, strings.Join(values, ","))
		}
	}
	testRPC := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		response, err := client.Call(
			ctx,
			"getAttr",
			map[string]any{"keys": []string{"deviceId", "fwVer"}},
			12_001,
			false,
		)
		if err != nil {
			fail(err.Error())
			return
		}
		data, err := response.AsMap()
		if err != nil {
			fail(err.Error())
			return
		}
		var attributes any
		if format == sdk.Protobuf {
			getAttr, _ := data["getAttr"].(map[string]any)
			attributes = getAttr["attributes"]
		} else {
			attributes = data["data"]
		}
		attributeMap, _ := attributes.(map[string]any)
		if attributeMap["deviceId"] != "DEMO001" {
			fail(fmt.Sprintf("RPC getAttr.deviceId 不匹配: %v", attributeMap["deviceId"]))
			return
		}
		mu.Lock()
		rpcPassed = true
		mu.Unlock()
		fmt.Printf("RPC_PASS consumer=%s method=getAttr req_id=12001 data=%v\n", name, data)
		completeIfReady()
	}

	handler := func(message *sdk.DecodedPayload) {
		if _, ok := requiredSuffixes[message.Suffix]; !ok {
			fail("未支持的 Topic: " + message.Suffix)
			return
		}
		data, err := message.AsMap()
		if err != nil {
			fail(err.Error())
			return
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			fail(err.Error())
			return
		}
		fmt.Printf("DECODED consumer=%s profile=%s topic=%s bytes=%d data=%s\n",
			name, format, message.Suffix, len(message.Raw), jsonData)
		startRPC := false
		mu.Lock()
		if !resultSent {
			seen[message.Suffix] = struct{}{}
			if message.Suffix == "telemetry" && !rpcStarted {
				rpcStarted = true
				startRPC = true
			}
		}
		mu.Unlock()
		if startRPC {
			go testRPC()
		}
		completeIfReady()
	}

	client, err = sdk.NewClient(sdk.Config{
		Host:           env("MQTT_HOST", "127.0.0.1"),
		Port:           port,
		Topics:         topics,
		PayloadFormat:  format,
		ClientID:       fmt.Sprintf("vdm-example-%s-%s", name, runID),
		QoS:            1,
		ConnectTimeout: time.Duration(timeoutSeconds * float64(time.Second)),
	}, handler, func(err error) { fail(err.Error()) })
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer client.Close()
	if err := client.PublishRaw(controlTopic("ready"), "READY", true); err != nil {
		return fmt.Errorf("发布就绪消息失败: %w", err)
	}
	fmt.Printf("READY consumer=%s profile=%s\n", name, format)
	select {}
}
