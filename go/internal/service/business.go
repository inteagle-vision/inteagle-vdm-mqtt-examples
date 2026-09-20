package service

import (
	"context"
	"errors"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

// Explicit domain dispatch prevents accidentally exposing a generic arbitrary RPC endpoint.
func (s *Service) invokeDomain(ctx context.Context, d Device, op Operation, p map[string]any) (*sdk.DecodedPayload, error) {
	switch op.Module {
	case "device":
		return s.deviceRPC(ctx, d, op, p)
	case "targets":
		return s.targetsRPC(ctx, d, op, p)
	case "measurement":
		return s.measurementRPC(ctx, d, op, p)
	case "alarms":
		return s.alarmsRPC(ctx, d, op, p)
	case "evidence":
		return s.evidenceRPC(ctx, d, op, p)
	}
	return nil, errors.New("unknown business module")
}
func (s *Service) call(ctx context.Context, d Device, op Operation, p map[string]any) (*sdk.DecodedPayload, error) {
	return s.clients[d.ConnectionID+"/"+d.DeviceID].Call(ctx, op.Method, p, 0, false)
}
