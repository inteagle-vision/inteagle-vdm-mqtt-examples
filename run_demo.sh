#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
profile="${1:-protobuf}"
if [[ "$profile" != protobuf && "$profile" != json ]]; then
    echo "用法: VDM_DEVICE_ID=<设备ID> MQTT_HOST=<Broker地址> $0 [protobuf|json] [--evidence]" >&2
    exit 2
fi
if [[ -z "${VDM_DEVICE_ID:-}" || "$VDM_DEVICE_ID" == YOUR_DEVICE_ID ]]; then
    echo "请设置 VDM_DEVICE_ID 为真实设备 ID。" >&2
    exit 2
fi
if [[ "$VDM_DEVICE_ID" == *['/+#']* ]]; then
    echo "VDM_DEVICE_ID 不能包含 /、+ 或 #。" >&2
    exit 2
fi
if (( $# > 2 )) || [[ -n "${2:-}" && "${2:-}" != --evidence ]]; then
    echo "第二个参数仅支持 --evidence。" >&2
    exit 2
fi
export VDM_PAYLOAD_FORMAT="$profile"
sha256sum --check proto/SHA256SUMS
compose_args=()
if [[ "${2:-}" == --evidence ]]; then
    compose_args+=(--profile evidence)
fi
if [[ -n "${MQTT_HOST:-}" && "$MQTT_HOST" != nanomq ]]; then
    services=(python-data go-data java-data javascript-data)
    if [[ "${2:-}" == --evidence ]]; then
        services+=(python-evidence go-evidence java-evidence javascript-evidence)
    fi
    printf '连接指定 MQTT Broker，启动接收程序，格式=%s。\n' "$profile"
    exec docker compose "${compose_args[@]}" up --build -d --no-deps "${services[@]}"
fi
if [[ -z "${NANOMQ_BIND_ADDRESS:-}" || "$NANOMQ_BIND_ADDRESS" == 127.0.0.1 ]]; then
    echo "自建 NanoMQ 时，请设置 NANOMQ_BIND_ADDRESS 为本机可供设备访问的监听 IP。" >&2
    exit 2
fi
printf '启动 NanoMQ 与接收程序，格式=%s，Broker 端口=%s。\n' "$profile" "${NANOMQ_PORT:-18883}"
exec docker compose "${compose_args[@]}" up --build -d
