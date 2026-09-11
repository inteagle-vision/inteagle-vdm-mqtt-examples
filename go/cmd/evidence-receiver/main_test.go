package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

type assemblerFunc func(*sdk.EvidencePackageChunk) (*sdk.CompletedAlarmSnapshotPackage, error)

func (f assemblerFunc) Accept(chunk *sdk.EvidencePackageChunk) (*sdk.CompletedAlarmSnapshotPackage, error) {
	return f(chunk)
}
func noLog(string, ...any) {}
func complete(event uint64) *sdk.CompletedAlarmSnapshotPackage {
	return &sdk.CompletedAlarmSnapshotPackage{EventID: event, PackageSHA256: "package-hash", PackagePath: "saved.tar"}
}

func TestMQTTCallbackIsBoundedAndNeverWaitsForDisk(t *testing.T) {
	r := newReceiver(nil, nil, noLog)
	// Without a worker the queue fills. The next callback must return instead of
	// blocking the MQTT network loop or starting another background worker.
	done := make(chan struct{})
	go func() {
		for i := 0; i < chunkQueueSize+1; i++ {
			r.onMessage(&sdk.DecodedPayload{Value: &sdk.EvidencePackageChunk{EventID: 1}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MQTT callback blocked")
	}
	if len(r.chunks) != chunkQueueSize || len(r.errors) != 1 {
		t.Fatal("queue bound or overflow report missing")
	}
}

func TestWaitingForRPCDoesNotStopDiskProcessingAndACKQueueIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saved := make(chan uint64, 64)
	ackStarted := make(chan struct{})
	r := newReceiver(assemblerFunc(func(chunk *sdk.EvidencePackageChunk) (*sdk.CompletedAlarmSnapshotPackage, error) {
		saved <- chunk.EventID
		return complete(chunk.EventID), nil
	}), func(ctx context.Context, _ uint64, _ string) error {
		close(ackStarted)
		<-ctx.Done()
		return ctx.Err()
	}, noLog)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); r.receive(ctx) }()
	go func() { defer workers.Done(); r.acknowledgePackages(ctx) }()
	defer func() { cancel(); workers.Wait() }()
	r.onMessage(&sdk.DecodedPayload{Value: &sdk.EvidencePackageChunk{EventID: 1}})
	select {
	case <-ackStarted:
	case <-time.After(time.Second):
		t.Fatal("ACK not started")
	}
	for i := 2; i <= ackQueueSize+3; i++ {
		r.onMessage(&sdk.DecodedPayload{Value: &sdk.EvidencePackageChunk{EventID: uint64(i)}})
	}
	for i := 1; i <= ackQueueSize+3; i++ {
		select {
		case <-saved:
		case <-time.After(time.Second):
			t.Fatal("RPC wait blocked disk worker")
		}
	}
	select {
	case <-r.errors:
	case <-time.After(time.Second):
		t.Fatal("ACK overflow was not reported")
	}
	if len(r.acknowledgements) != ackQueueSize {
		t.Fatal("ACK queue is not bounded")
	}
}

func TestSuccessfulDuplicatePackagesAreAcknowledgedIdempotently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acknowledged := make(chan uint64, 4)
	r := newReceiver(nil, func(_ context.Context, event uint64, _ string) error { acknowledged <- event; return nil }, noLog)
	done := make(chan struct{})
	go func() { defer close(done); r.acknowledgePackages(ctx) }()
	defer func() { cancel(); <-done }()
	r.acknowledgements <- complete(1)
	r.acknowledgements <- complete(1)
	r.acknowledgements <- complete(2)
	for _, expected := range []uint64{1, 2} {
		select {
		case got := <-acknowledged:
			if got != expected {
				t.Fatalf("got %d expected %d", got, expected)
			}
		case <-time.After(time.Second):
			t.Fatal("ACK missing")
		}
	}
}

func TestFailedACKCanBeRetriedByDuplicateDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	succeeded := make(chan struct{})
	r := newReceiver(nil, func(context.Context, uint64, string) error {
		calls++
		if calls == 1 {
			return &sdk.RPCError{Code: 2, Message: "invalid request"}
		}
		close(succeeded)
		return nil
	}, noLog)
	done := make(chan struct{})
	go func() { defer close(done); r.acknowledgePackages(ctx) }()
	defer func() { cancel(); <-done }()
	r.acknowledgements <- complete(1)
	r.acknowledgements <- complete(1)
	select {
	case <-succeeded:
	case <-time.After(time.Second):
		t.Fatal("failed ACK was incorrectly deduplicated")
	}
}

func TestACKRetriesTransientFailuresWithoutRedelivery(t *testing.T) {
	failures := []error{&sdk.RPCError{Code: 4}, &sdk.RPCError{Code: 5}, &sdk.TransportError{Err: errors.New("connection lost")}}
	calls := 0
	var delays []time.Duration
	err := acknowledgeWithRetry(context.Background(), func(ctx context.Context, event uint64, hash string) error {
		if event != 42 || hash != "package-hash" {
			t.Fatal("retry changed package identity")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("attempt has no timeout")
		}
		calls++
		if calls <= len(failures) {
			return failures[calls-1]
		}
		return nil
	}, complete(42), func(_ context.Context, delay time.Duration) error { delays = append(delays, delay); return nil })
	if err != nil || calls != 4 || !reflect.DeepEqual(delays, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}) {
		t.Fatalf("calls=%d delays=%v err=%v", calls, delays, err)
	}
}

func TestACKRetryLimitAndPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		calls int
	}{
		{"rate limited", &sdk.RPCError{Code: 4}, 4},
		{"timeout", context.DeadlineExceeded, 4},
		{"invalid parameters", &sdk.RPCError{Code: 2}, 1},
		{"unsupported", &sdk.RPCError{Code: 3}, 1},
		{"resource changed", &sdk.RPCError{Code: 6}, 1},
		{"cancelled", context.Canceled, 1},
		{"local validation", errors.New("invalid hash"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := acknowledgeWithRetry(context.Background(), func(context.Context, uint64, string) error { calls++; return fmt.Errorf("ACK: %w", tc.err) }, complete(1), func(context.Context, time.Duration) error { return nil })
			if !errors.Is(err, tc.err) || calls != tc.calls {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestACKRetryBackoffCanBeCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan struct{})
	done := make(chan error, 1)
	calls := 0
	go func() {
		done <- acknowledgeWithRetry(ctx, func(context.Context, uint64, string) error { calls++; return &sdk.RPCError{Code: 4} }, complete(1), func(ctx context.Context, delay time.Duration) error { close(waiting); return waitForRetry(ctx, delay) })
	}()
	<-waiting
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation did not interrupt ACK backoff")
	}
}
