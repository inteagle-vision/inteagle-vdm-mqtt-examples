#!/usr/bin/env python3
"""Receive, persist, verify, and acknowledge device alarm snapshot packages."""

import os
import queue
import signal
import sys
import threading
from collections import OrderedDict

from vdm_mqtt_sdk import (
    AlarmSnapshotPackageAssembler,
    EvidencePackageChunk,
    RpcError,
    VdmMqttClient,
    VdmMqttClientConfig,
    VdmTopics,
)


def acknowledge_with_retry(client, completed, stopped):
    for attempt in range(4):
        try:
            client.ack_evidence_package(completed.event_id, completed.package_sha256)
            return True
        except Exception as error:
            transient = (isinstance(error, RpcError) and error.code in (4, 5)) or isinstance(error, (TimeoutError, ConnectionError))
            if not transient or attempt == 3:
                raise
            print(f"ACK_RETRY eventId={completed.event_id} attempt={attempt + 2}", flush=True)
            if stopped.wait(2 ** attempt):
                return False
    return False


def main():
    for name in ("MQTT_HOST", "VDM_DEVICE_ID"):
        if not os.getenv(name):
            raise ValueError(f"Set {name}")
    assembler = AlarmSnapshotPackageAssembler(os.getenv("VDM_EVIDENCE_DIR", "./evidence"))
    chunks = queue.Queue(maxsize=64)
    confirmations = queue.Queue(maxsize=16)
    stopped = threading.Event()

    def report(error):
        print("ERROR", error, file=sys.stderr, flush=True)

    def on_message(message):
        if isinstance(message.value, EvidencePackageChunk):
            try:
                chunks.put_nowait(message.value)
            except queue.Full:
                report("Image queue full; use retryEvidence to resend the incomplete package")

    config = VdmMqttClientConfig(
        host=os.environ["MQTT_HOST"],
        port=int(os.getenv("MQTT_PORT", "1883")),
        topics=VdmTopics.for_device(os.environ["VDM_DEVICE_ID"]),
        payload_format=os.getenv("VDM_PAYLOAD_FORMAT", "protobuf"),
        username=os.getenv("MQTT_USERNAME"),
        password=os.getenv("MQTT_PASSWORD"),
        qos=1,
        subscription_suffixes=("image", "rpc/resp"),
    )
    with VdmMqttClient(config, on_message=on_message, on_error=report) as client:
        def persist():
            while not stopped.is_set():
                try:
                    chunk = chunks.get(timeout=0.2)
                except queue.Empty:
                    continue
                try:
                    completed = assembler.accept(chunk)
                    print(f"CHUNK eventId={chunk.event_id} chunk={chunk.chunk_index + 1}/{chunk.chunk_count}", flush=True)
                    if completed is not None:
                        print(f"VERIFIED eventId={completed.event_id} package={completed.package_path}", flush=True)
                        confirmations.put_nowait(completed)
                except queue.Full:
                    report("ACK queue full; use retryEvidence to request confirmation again")
                except Exception as error:
                    report(error)

        def acknowledge():
            acknowledged = OrderedDict()
            while not stopped.is_set():
                try:
                    completed = confirmations.get(timeout=0.2)
                except queue.Empty:
                    continue
                key = (completed.event_id, completed.package_sha256)
                if key in acknowledged:
                    continue
                try:
                    if not acknowledge_with_retry(client, completed, stopped):
                        continue
                    acknowledged[key] = True
                    if len(acknowledged) > 256:
                        acknowledged.popitem(last=False)
                    print(f"ACKED eventId={completed.event_id} packageSha256={completed.package_sha256} code=0", flush=True)
                except Exception as error:
                    print(f"ACK_FAILED eventId={completed.event_id} error={error}", file=sys.stderr, flush=True)

        workers = [threading.Thread(target=f, daemon=True) for f in (persist, acknowledge)]
        for worker in workers:
            worker.start()
        print("READY evidence receiver; Ctrl-C to stop", flush=True)
        try:
            stopped.wait()
        finally:
            stopped.set()
            client.stop()
            for worker in workers:
                worker.join(timeout=2)


if __name__ == "__main__":
    def terminate(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, terminate)
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print("ERROR", error, file=sys.stderr)
        sys.exit(1)
