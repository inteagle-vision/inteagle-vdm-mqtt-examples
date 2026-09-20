#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Independent brokers and ephemeral data; no real-device dependency.
image=emqx/nanomq:0.25.2-2@sha256:8d391c73eb084a32accbe13516bbea83e33736542da9ea755c3b3b3417378961
name_a="vdm-contract-a-$$"
name_b="vdm-contract-b-$$"
cleanup() { docker rm -f "$name_a" "$name_b" >/dev/null 2>&1 || true; }
trap cleanup EXIT
# Let Docker choose free loopback ports so parallel CI runs do not collide.
docker run --detach --rm --name "$name_a" -p 127.0.0.1::1883 "$image" >/dev/null
docker run --detach --rm --name "$name_b" -p 127.0.0.1::1883 "$image" >/dev/null
export VDM_TEST_BROKER_A="$(docker port "$name_a" 1883/tcp | cut -d: -f2)"
export VDM_TEST_BROKER_B="$(docker port "$name_b" 1883/tcp | cut -d: -f2)"
node tests/services_contract.js "$@"
