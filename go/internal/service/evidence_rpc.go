package service

import (
	"context"
	"database/sql"
	"errors"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"os"
)

type InvalidArgument struct{ Message string }

func (e *InvalidArgument) Error() string { return e.Message }

// An HTTP caller can acknowledge only a package this service has verified.
// The SDK still exposes direct ACK for independently verified customer receivers.
func (s *Service) evidenceRPC(ctx context.Context, d Device, op Operation, p map[string]any) (*sdk.DecodedPayload, error) {
	if op.Method == "ackEvidencePackage" {
		event, ok := p["eventId"].(string)
		hash, hashOK := p["packageSha256"].(string)
		kind, kindOK := p["kind"].(string)
		if !ok || !hashOK || !kindOK || (kind != "SNAPSHOT" && kind != "EVIDENCE_KIND_SNAPSHOT") {
			return nil, &InvalidArgument{"eventId, snapshot kind and packageSha256 required"}
		}
		var packagePath string
		e := s.db.QueryRowContext(ctx, "SELECT path FROM evidence_receipts WHERE connection=? AND device=? AND event_id=? AND hash=?", d.ConnectionID, d.DeviceID, event, hash).Scan(&packagePath)
		if errors.Is(e, sql.ErrNoRows) {
			return nil, &InvalidArgument{"no locally verified receipt matches this device, eventId and packageSha256"}
		}
		if e != nil {
			return nil, e
		}
		if _, e = os.Stat(packagePath); e != nil {
			return nil, &InvalidArgument{"verified package is no longer available locally"}
		}
	}
	return s.call(ctx, d, op, p)
}
