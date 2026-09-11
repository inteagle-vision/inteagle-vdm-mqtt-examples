#!/usr/bin/env python3
"""Run one MQTT alarm RPC. No arguments means read-only capabilities."""
import argparse
import json
import os
from pathlib import Path
import secrets
import time
from vdm_mqtt_sdk import (
    PayloadFormat,
    VdmCodec,
    VdmMqttClient,
    VdmMqttClientConfig,
    VdmTopics,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--case", default="capabilities", help="Name in examples/alarms/requests.json"
    )
    source.add_argument(
        "--request",
        type=Path,
        help="Your complete {method,params} JSON file for the selected format",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Validate encoding and print without connecting",
    )
    parser.add_argument(
        "--listen",
        type=float,
        default=0,
        help="Seconds to receive 3A/event after the RPC",
    )
    args = parser.parse_args()
    if args.listen < 0:
        parser.error("--listen must be nonnegative")
    profile = PayloadFormat.parse(os.getenv("VDM_PAYLOAD_FORMAT", "protobuf")).value
    if args.request:
        request = json.loads(args.request.read_text())
    else:
        cases = json.loads(
            (
                Path(__file__).resolve().parents[1] / "examples/alarms/requests.json"
            ).read_text()
        )
        case = next((item for item in cases if item["name"] == args.case), None)
        if case is None:
            parser.error("Unknown case: " + args.case)
        request = {"method": case["method"], "params": case[profile]}
    req_id = secrets.randbelow(2**31 - 1) + 1
    codec = VdmCodec(profile)
    payload, _ = codec.encode_rpc_request(request["method"], request["params"], req_id)
    print(json.dumps(request, ensure_ascii=False, indent=2))
    print(f"format={profile} bytes={len(payload)} reqId={req_id}")
    if args.print_only:
        return

    def on_message(message):
        if message.suffix in ("3A", "event"):
            # Do not call synchronous RPC from Paho's network callback.
            print(
                message.suffix,
                json.dumps(message.as_dict(), ensure_ascii=False),
                flush=True,
            )

    with VdmMqttClient(
        VdmMqttClientConfig(
            host=os.environ["MQTT_HOST"],
            port=int(os.getenv("MQTT_PORT", "1883")),
            topics=VdmTopics.for_device(os.environ["VDM_DEVICE_ID"]),
            payload_format=profile,
            username=os.getenv("MQTT_USERNAME"),
            password=os.getenv("MQTT_PASSWORD"),
        ),
        on_message=on_message,
        on_error=lambda error: print("DECODE_ERROR", error, flush=True),
    ) as client:
        response = client.call(
            request["method"], request["params"], req_id=req_id, timeout=30
        )
        print(
            "RESPONSE", json.dumps(response.as_dict(), ensure_ascii=False), flush=True
        )
        if args.listen:
            time.sleep(args.listen)


if __name__ == "__main__":
    main()
