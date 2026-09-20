package businessexamples

import (
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"testing"
)

func TestEveryExampleEncodesBothFormats(t *testing.T) {
	for _, f := range []sdk.PayloadFormat{sdk.JSON, sdk.Protobuf} {
		for _, name := range []string{"device", "targets", "measurement", "measurement-status", "measurement-cancel", "alarms", "evidence"} {
			r, e := Select(name, f)
			if e != nil {
				t.Fatal(e)
			}
			if len(r.Params) == 0 {
				t.Fatal("empty placeholder parameters")
			}
			if _, _, e = (sdk.Codec{Format: f}).EncodeRPC(r.Method, r.Params, 1); e != nil {
				t.Fatal(name, f, e)
			}
		}
	}
}
