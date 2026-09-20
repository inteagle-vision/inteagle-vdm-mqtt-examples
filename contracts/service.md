# Shared service contract v1

User-approved implementation plan: VDM 多语言接入示例重构计划, 2026-09-20.
Work is in the MQTT examples repository; device firmware is read-only.

## Configuration
`VDM_SERVICE_CONFIG` points to a JSON file (default `../contracts/config.example.json`
when starting inside a language directory). `VDM_DATA_DIR` defaults `./data`;
`VDM_HTTP_HOST` defaults `127.0.0.1`; `VDM_HTTP_PORT` defaults `8080`.
Non-loopback bind REQUIRES `VDM_API_TOKEN`; when set all routes except /health
require `Authorization: Bearer <token>`. `VDM_WEBHOOK_URL` optional;
`VDM_WEBHOOK_TOKEN` optional bearer secret from environment only.
Config: `{"connections":[{"id":"local","host":"127.0.0.1","port":1883,
"usernameEnv":"MQTT_USERNAME","passwordEnv":"MQTT_PASSWORD",
"devices":[{"id":"DEMO","format":"json","capabilities":[]}]}],
"rpcTimeoutMs":10000,"notificationMaxAgeSeconds":300,"maxInboxRows":10000,
"retentionDays":7,"maxEvidenceBytes":268435456}`.
IDs match `[A-Za-z0-9_-]{1,128}`. Duplicate connection IDs/device IDs within a
connection are invalid. One SDK client per connection/device, unique stable
client ID per tuple and language, persistent session, manual QoS1 acknowledgement.
No cross-connection pending map. Connection failures keep HTTP alive/degraded
and reconnect in background. No real credentials/addresses in tracked files.

## HTTP
GET /health -> 200 {"status":"ok"|"degraded","connections":[{"connectionId":...,"deviceId":...,"connected":true}],"counters":{...}}
GET /v1/devices -> 200 {"devices":[{"connectionId":...,"deviceId":...,"format":...,"capabilities":[]}]}
GET /v1/connections/{connectionId}/devices/{deviceId}/latest -> 200 {"telemetry":null|object,"attributes":null|object,"receivedCount":integer}
GET /v1/connections/{connectionId}/devices/{deviceId}/alarms/local -> 200 {"items":[...persisted lifecycle states...]}
POST /v1/connections/{connectionId}/devices/{deviceId}/{route} with
{"params":{...}} (omitted params = {}). Route metadata in operations.json;
there is no unrestricted RPC REST endpoint. Params use device JSON for JSON
connections, ProtoJSON for Protobuf connections (64-bit IDs decimal strings).
200 {"connectionId":...,"deviceId":...,"method":...,"status":"completed","response":<decoded device RPC response>}
202 same envelope with status "accepted" ONLY for async operations AFTER successful device RPC.
Errors {"error":{"code":...,"message":...,"outcome":"unknown"? ,"deviceCode":integer?}}
400 INVALID_ARGUMENT; 404 NOT_FOUND; 409 UNSUPPORTED_CAPABILITY;
502 DEVICE_ERROR (nonzero RPC code); 503 UNAVAILABLE/OVERLOADED;
504 RPC_TIMEOUT with outcome unknown (never retry modifying RPC automatically).
Unknown device rejected before calling SDK. Timeout bounded 100..60000ms.
REST body max 1 MiB; reject arrays/nonobjects/unknown top-level fields.
Capabilities are explicitly configured verified hardware capabilities, not inferred
from model names. Operations with required capability reject absent capability.
Device nonzero capability codes are still surfaced as DEVICE_ERROR.

## Durability and workers
SQLite WAL, synchronous FULL, user_version migration 1. Single process instance
per data directory. On MQTT business messages, insert bounded persistent inbox
(raw payload, topic, source tuple) synchronously before PUBACK. RPC response
resolution is immediate and bypasses inbox. QoS0 best effort only. Failure or
capacity exhaustion must not ACK QoS1; log/count then reconnect for redelivery.
A worker processes durable inbox with bounded retry/backoff, terminal failed
records visible through health counters. Never execute RPC on MQTT callback.
Telemetry/attributes overwrite latest and increment receive count.
Alarm event uniqueness (connectionId,deviceId,eventId), lifecycle by alarmId,
event insert/state/outbox in one transaction. Normalize enum prefixes.
Only strictly newer ts updates state; equal/older events retained, mark lifecycle
needsReconcile and durably schedule getAlarmState; same timestamp is ambiguous.
SYNCED state-only; notify TRIGGERED, ESCALATED, RECOVERED, CANCELLED, never
DEESCALATED. Suppress stale (>configured age) and future timestamp notifications.
Stable outbox key SHA256(JSON array [connectionId,deviceId,eventId,transition]);
Webhook POST with Idempotency-Key, retries bounded (8), exponential backoff,
timeout 5s. Downstream must deduplicate; at-least-once, no exactly-once promise.
/event evidence states are separate, identity includes payload/state, never
creates alarm outbox. Image chunks persisted in inbox before ACK and assembled
using SDK bounds/hash/safe tar validation; receipt durable before application
ackEvidencePackage RPC. Failed ACK retried durably, no ACK before verification.
Ordinary JPEG also saved with bounded capacity. Prune processed inbox/events/
finished outbox older than retentionDays; never prune pending work. Bound total
evidence disk usage with maxEvidenceBytes; capacity exhaustion reported.

## Refined acceptance policies
- Notification policy: a fresh TRIGGERED notifies only when this alarmId has no prior
  lifecycle. ESCALATED notifies when there is no prior lifecycle or its level is
  strictly higher than the previous level. Fresh RECOVERED/CANCELLED notify.
- Pending/failed notification outbox is bounded by maxInboxRows. Exhaustion rolls
  back event/state/outbox together, retaining the durable inbox task for retry.
- Processed alarm/evidence history has a capacity of 10 × maxInboxRows, oldest first,
  in addition to retentionDays. Deduplication is bounded by both windows.
- `/alarms/local` keeps last event fields top-level and `needsReconcile` boolean.
  A successful query adds `currentActive`, `currentState` (object or null), and
  `reconciledAt` (platform Unix seconds). Query time never changes device event ts.
- REST evidence/ack requires a matching verified local receipt and existing package
  for the same connection/device/event/hash. SDK ACK remains available for custom
  receivers that establish their own durable verification boundary.
- Ordinary image ingress permits at most 32 MiB within maxEvidenceBytes; other
  MQTT payloads and REST bodies are limited to 1 MiB. Image capacity includes
  queued/partial/complete storage (SQLite allocation overhead requires headroom).
- retentionDays removes old ordinary JPEG and completed application-ACKed evidence;
  partial/pending/failed evidence is retained for diagnosis/recovery. Removing a
  package invalidates its local ACK eligibility.
