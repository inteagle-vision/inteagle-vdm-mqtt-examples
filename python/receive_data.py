#!/usr/bin/env python3
"""Receive device telemetry and attributes; optionally query attributes once."""

import argparse
import json
import os
import secrets
import signal
import sys
import threading

from vdm_mqtt_sdk import VdmMqttClient, VdmMqttClientConfig, VdmTopics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query-attributes",
        action="store_true",
        help="Call getAttr once from the main thread, then keep receiving data",
    )
    args = parser.parse_args()
    for name in ("MQTT_HOST", "VDM_DEVICE_ID"):
        if not os.getenv(name):
            parser.error(f"Set {name}")

    subscriptions = ("telemetry", "attributes")
    if args.query_attributes:
        subscriptions += ("rpc/resp",)

    def on_message(message):
        if message.suffix in ("telemetry", "attributes"):
            print(
                message.suffix,
                json.dumps(message.as_dict(), ensure_ascii=False),
                flush=True,
            )

    config = VdmMqttClientConfig(
        host=os.environ["MQTT_HOST"],
        port=int(os.getenv("MQTT_PORT", "1883")),
        topics=VdmTopics.for_device(os.environ["VDM_DEVICE_ID"]),
        payload_format=os.getenv("VDM_PAYLOAD_FORMAT", "protobuf"),
        username=os.getenv("MQTT_USERNAME"),
        password=os.getenv("MQTT_PASSWORD"),
        subscription_suffixes=subscriptions,
    )
    with VdmMqttClient(
        config,
        on_message=on_message,
        on_error=lambda error: print(
            "DECODE_ERROR", error, file=sys.stderr, flush=True
        ),
    ) as client:
        print("READY waiting for telemetry/attributes; Ctrl-C to stop", flush=True)
        if args.query_attributes:
            response = client.call(
                "getAttr",
                {"keys": ["deviceId", "deviceModel", "fwVer", "measureStatus"]},
                req_id=secrets.randbelow(2**31 - 1) + 1,
                timeout=30,
            )
            fields = response.as_dict()
            fields.setdefault("code", 0)  # Protobuf omits default-valued scalar fields.
            print("RESPONSE", json.dumps(fields, ensure_ascii=False), flush=True)
        threading.Event().wait()


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
