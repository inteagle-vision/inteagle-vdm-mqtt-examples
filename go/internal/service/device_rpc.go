package service

import (
	"context"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

// deviceRPC is the device business boundary; transport owns request correlation.
func (s *Service) deviceRPC(ctx context.Context, d Device, op Operation, p map[string]any) (*sdk.DecodedPayload, error) {
	return s.call(ctx, d, op, p)
}
