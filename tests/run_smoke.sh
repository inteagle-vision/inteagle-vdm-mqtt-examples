#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-protobuf}"

if [[ "${MODE}" != "protobuf" && "${MODE}" != "json" && "${MODE}" != "all" ]]; then
    echo "用法: $0 [protobuf|json|all]" >&2
    exit 2
fi

cd "${SCRIPT_DIR}"
sha256sum --check proto/SHA256SUMS

ACTIVE_PROJECT=""
cleanup() {
    if [[ -n "${ACTIVE_PROJECT}" ]]; then
        docker compose -f tests/compose.smoke.yaml -p "${ACTIVE_PROJECT}" down --volumes --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

run_profile() {
    local profile="$1"
    local run_id="${profile}-$$"
    local project="vdm-mqtt-${profile}-$$"
    ACTIVE_PROJECT="${project}"

    echo "启动 VDM MQTT ${profile} 示例（NanoMQ + Python + Go + Java + JavaScript）"
    export VDM_PAYLOAD_FORMAT="${profile}"
    export VDM_RUN_ID="${run_id}"

    local exit_code=0
    docker compose -f tests/compose.smoke.yaml -p "${project}" up --build --exit-code-from publisher || exit_code=$?
    docker compose -f tests/compose.smoke.yaml -p "${project}" down --volumes --remove-orphans
    ACTIVE_PROJECT=""

    if [[ "${exit_code}" -ne 0 ]]; then
        echo "VDM MQTT ${profile} 示例验证失败" >&2
        return "${exit_code}"
    fi
    echo "VDM MQTT ${profile} 示例验证通过"
}

if [[ "${MODE}" == "all" ]]; then
    run_profile protobuf
    run_profile json
else
    run_profile "${MODE}"
fi
