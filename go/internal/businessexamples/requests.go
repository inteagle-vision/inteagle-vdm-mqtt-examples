// Package businessexamples supplies concrete device requests, not REST calls.
package businessexamples

import (
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

type Request struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

func Select(name string, format sdk.PayloadFormat) (Request, error) {
	p := map[string]any{}
	method := ""
	switch name {
	case "device":
		method = "getAttr"
		p = map[string]any{"keys": []string{"sampleFrequencyHz", "showTimestamp"}}
	case "targets":
		method = "setTargets"
		p = map[string]any{"targets": []any{map[string]any{"targetId": "T01", "skipMeasurement": true}}}
	case "measurement":
		method = "syncTelemetry"
		p = map[string]any{"type": "displacement", "startTs": 1788170700, "endTs": 1788181500, "targetIds": []string{"T01"}}
		if format == sdk.Protobuf {
			p = map[string]any{"telemetryType": "TELEMETRY_SYNC_TYPE_DISPLACEMENT", "startTs": "1788170700", "endTs": "1788181500", "targetIds": []string{"T01"}}
		}
	case "measurement-status":
		method = "getSyncStatus"
		p = map[string]any{"jobId": "550e8400-e29b-41d4-a716-446655440000"}
	case "measurement-cancel":
		method = "cancelSync"
		p = map[string]any{"jobId": "550e8400-e29b-41d4-a716-446655440000"}
	case "alarms":
		method = "listAlarmEvents"
		p = map[string]any{"page": 1, "pageSize": 20, "alarmId": "9007199254740900"}
	case "evidence":
		method = "getEvidenceStatus"
		p = map[string]any{"eventId": "9007199254740993", "kind": "SNAPSHOT"}
		if format == sdk.Protobuf {
			p["kind"] = "EVIDENCE_KIND_SNAPSHOT"
		}
	default:
		return Request{}, fmt.Errorf("unknown example %q", name)
	}
	return Request{method, p}, nil
}
