// Receive and durably validate alarm image packages, then acknowledge them by MQTT RPC.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/internal/exampleconfig"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
)

const chunkQueueSize = 32 // At most 4 MiB of chunk bytes, in addition to SDK decoding.
const ackQueueSize = 16

type packageAssembler interface {
	Accept(*sdk.EvidencePackageChunk) (*sdk.CompletedAlarmSnapshotPackage, error)
}
type acknowledgeFunc func(context.Context, uint64, string) error

type receiver struct {
	chunks           chan *sdk.EvidencePackageChunk
	acknowledgements chan *sdk.CompletedAlarmSnapshotPackage
	errors           chan error
	assembler        packageAssembler
	acknowledge      acknowledgeFunc
	log              func(string, ...any)
}

func newReceiver(assembler packageAssembler, acknowledge acknowledgeFunc, log func(string, ...any)) *receiver {
	return &receiver{chunks: make(chan *sdk.EvidencePackageChunk, chunkQueueSize),
		acknowledgements: make(chan *sdk.CompletedAlarmSnapshotPackage, ackQueueSize),
		errors:           make(chan error, 16), assembler: assembler, acknowledge: acknowledge, log: log}
}

func (r *receiver) report(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

// No disk I/O, logging or RPC wait runs on the MQTT callback.
func (r *receiver) onMessage(message *sdk.DecodedPayload) {
	chunk, ok := message.Value.(*sdk.EvidencePackageChunk)
	if !ok {
		return
	}
	select {
	case r.chunks <- chunk:
	default:
		r.report(fmt.Errorf("chunk queue full for eventId=%d; run retryEvidence after the receiver catches up", chunk.EventID))
	}
}

func (r *receiver) receive(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case chunk := <-r.chunks:
			completed, err := r.assembler.Accept(chunk)
			if err != nil {
				r.report(fmt.Errorf("eventId=%d: %w", chunk.EventID, err))
				continue
			}
			r.log("CHUNK eventId=%d chunk=%d/%d\n", chunk.EventID, chunk.ChunkIndex+1, chunk.ChunkCount)
			if completed == nil {
				continue
			}
			r.log("VERIFIED eventId=%d package=%s\n", completed.EventID, completed.PackagePath)
			select {
			case r.acknowledgements <- completed:
			default:
				r.report(fmt.Errorf("ACK queue full for eventId=%d; package is saved, run retryEvidence to request redelivery", completed.EventID))
			}
		}
	}
}

func (r *receiver) acknowledgePackages(ctx context.Context) {
	// Bound successful duplicate suppression to the most recent 64 packages.
	seen := make(map[string]struct{})
	order := make([]string, 0, 64)
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case completed := <-r.acknowledgements:
			key := fmt.Sprintf("%d/%s", completed.EventID, completed.PackageSHA256)
			if _, exists := seen[key]; exists {
				continue
			}
			err := acknowledgeWithRetry(ctx, r.acknowledge, completed, waitForRetry)
			if err != nil {
				r.report(fmt.Errorf("ACK_FAILED eventId=%d: %w; package is saved; retry ackEvidencePackage with this eventId and packageSha256", completed.EventID, err))
				continue
			}
			if len(order) == 64 {
				delete(seen, order[0])
				order = order[1:]
			}
			seen[key] = struct{}{}
			order = append(order, key)
			r.log("ACKED eventId=%d packageSha256=%s code=0\n", completed.EventID, completed.PackageSHA256)
		}
	}
}

// Only transient failures are retried. Parameter and other device RPC errors
// require caller action and must not keep the ACK worker occupied.
func retryableACKError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var rpcError *sdk.RPCError
	if errors.As(err, &rpcError) {
		return rpcError.Code == 4 || rpcError.Code == 5
	}
	var transportError *sdk.TransportError
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &transportError)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func acknowledgeWithRetry(ctx context.Context, acknowledge acknowledgeFunc, completed *sdk.CompletedAlarmSnapshotPackage, wait func(context.Context, time.Duration) error) error {
	for attempt := 0; attempt < 4; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		ackCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := acknowledge(ackCtx, completed.EventID, completed.PackageSHA256)
		cancel()
		if err == nil || !retryableACKError(err) || attempt == 3 {
			return err
		}
		if err := wait(ctx, time.Second*time.Duration(1<<attempt)); err != nil {
			return err
		}
	}
	panic("unreachable ACK retry state")
}

func run() error {
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	config, err := exampleconfig.Load("image", "rpc/resp")
	if err != nil {
		return err
	}
	assembler, err := sdk.NewAlarmSnapshotPackageAssembler(exampleconfig.Env("VDM_EVIDENCE_DIR", "./evidence"), 8)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var client *sdk.Client
	receiver := newReceiver(assembler, func(ctx context.Context, eventID uint64, hash string) error {
		_, err := client.AckEvidencePackage(ctx, eventID, hash)
		return err
	}, func(format string, values ...any) { fmt.Printf(format, values...) })
	client, err = sdk.NewClient(config, receiver.onMessage, receiver.report)
	if err != nil {
		return err
	}
	defer client.Close()
	workersCtx, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); receiver.receive(workersCtx) }()
	go func() { defer workers.Done(); receiver.acknowledgePackages(workersCtx) }()
	defer func() { cancelWorkers(); workers.Wait() }()
	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = client.Start(startCtx)
	cancel()
	if err != nil {
		return err
	}
	fmt.Printf("READY evidence-receiver deviceId=%s format=%s output=%s\n", os.Getenv("VDM_DEVICE_ID"), config.PayloadFormat, exampleconfig.Env("VDM_EVIDENCE_DIR", "./evidence"))
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-receiver.errors:
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
