// Package exampleconfig supplies the shared environment settings of real-device examples.
package exampleconfig

import (
	"fmt"
	"os"
	"strconv"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

func Env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func Load(suffixes ...string) (sdk.Config, error) {
	host, device := os.Getenv("MQTT_HOST"), os.Getenv("VDM_DEVICE_ID")
	if host == "" || device == "" {
		return sdk.Config{}, fmt.Errorf("set MQTT_HOST and VDM_DEVICE_ID")
	}
	port, err := strconv.Atoi(Env("MQTT_PORT", "1883"))
	if err != nil || port < 1 || port > 65535 {
		return sdk.Config{}, fmt.Errorf("MQTT_PORT must be between 1 and 65535")
	}
	topics, err := sdk.TopicsForDevice(device)
	if err != nil {
		return sdk.Config{}, err
	}
	format, err := sdk.ParsePayloadFormat(Env("VDM_PAYLOAD_FORMAT", "protobuf"))
	if err != nil {
		return sdk.Config{}, err
	}
	return sdk.Config{Host: host, Port: port, Topics: topics, PayloadFormat: format,
		Username: os.Getenv("MQTT_USERNAME"), Password: os.Getenv("MQTT_PASSWORD"), QoS: 1,
		SubscriptionSuffixes: suffixes}, nil
}
