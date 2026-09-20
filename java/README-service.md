# Java VDM reference service

Java 21 + Spring Boot 3.4.13; version compatibility is documented in [Spring's system requirements](https://docs.spring.io/spring-boot/3.4/system-requirements.html). Dependencies and build plugins are pinned in `pom.xml`. This is an integration reference, not a claim that every method was exercised on every hardware model.

From `java/`:

```sh
mvn --batch-mode package
cp ../contracts/config.example.json /tmp/vdm-java.json
export VDM_SERVICE_CONFIG=/tmp/vdm-java.json
export VDM_DATA_DIR=./data
# Set MQTT_USERNAME and MQTT_PASSWORD when the broker requires them.
java -jar target/vdm-mqtt-consumer-1.0.0-service.jar
```

Edit the configuration's broker/device tuples first. Credentials are read only from the named environment variables. IDs must match `[A-Za-z0-9_-]{1,128}`; select `json` or `protobuf` per device, and explicitly list verified `motor` capability when exposing motor/cruise routes. The shared operation matrix decides each route’s capability requirement. Default bind is `127.0.0.1:8080`; `VDM_HTTP_HOST` / `VDM_HTTP_PORT` override it. A non-loopback bind requires `VDM_API_TOKEN`; when present send `Authorization: Bearer $VDM_API_TOKEN` on every endpoint except `/health`.

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/devices
curl -X POST http://127.0.0.1:8080/v1/connections/local/devices/DEMO/device/attributes/query \
  -H 'Content-Type: application/json' -d '{"params":{"keys":["sampleFrequencyHz"]}}'
```

The exact 33 routes, parameters and status semantics are shared in `../contracts/service.md` and `../contracts/operations.json`. JSON devices accept device JSON; Protobuf devices accept ProtoJSON, including decimal **strings** for uint64 IDs. Only successful device responses yield 200/202; a timeout is 504 with `outcome: unknown`. Never automatically retry a modifying operation after such an outcome.

`device/`, `targets/`, `measurement/`, `alarms/`, and `evidence/` contain business controllers. `RpcGateway` enforces connection isolation, capability checks and a 32-call concurrency limit. `DeviceRegistry` creates a persistent-session SDK per tuple. Broker outages keep the HTTP server running with degraded health and background reconnects.

`Store` acquires an exclusive data-directory lock, applies schema version 1, and uses SQLite WAL with FULL synchronous writes. Raw business packets enter the bounded persistent inbox (old completed rows are evicted early when capacity is needed) before manual QoS1 PUBACK. RPC responses bypass that queue. Four worker loops process inbox, reconcile/ACK jobs, webhook delivery, and retention. Inbox/job/webhook errors retry up to eight attempts with backoff; terminal failures and pending counts appear in `/health`. Failed messages remain available in SQLite for operator diagnosis. HTTP bodies and non-image MQTT payloads are limited to 1 MiB. Image MQTT payloads may use up to `min(32 MiB, maxEvidenceBytes)`; pending raw image bytes and evidence files share the evidence quota. JPEG processing accounts for the current raw row being retired after its durable file write. `/latest` is durable, and `/alarms/local` includes reconciliation flags.

Alarm events deduplicate by connection/device/event ID. Only strictly newer device timestamps alter lifecycle state. Equal/older events queue `getAlarmState`; results are fenced against concurrent events. Only the first TRIGGERED for an alarm ID notifies; ESCALATED notifies when there is no previous lifecycle or the level strictly increases (ALERT < ALARM < ACTION). Fresh RECOVERED/CANCELLED events notify. SYNCED and DEESCALATED do not notify. Stale/future notifications are suppressed. Outbox records are committed with event/state writes. Pending/failed notifications are bounded by `maxInboxRows`; capacity exhaustion rolls back the alarm transaction and leaves the durable inbox task for retry. Set `VDM_WEBHOOK_URL` and optionally `VDM_WEBHOOK_TOKEN` to deliver notifications; every delivery carries a stable SHA-256 `Idempotency-Key`. Deliveries are at-least-once, so receivers must deduplicate. Without a webhook URL, outbox records remain pending.

Evidence `/event` status messages stay separate from alarm lifecycle notifications. Image chunks stay durable while incomplete, replay on restart, and pass the SDK's SHA-256 and safe USTAR checks. The SDK fsyncs the package/receipt before a persistent `ackEvidencePackage` job is created. ACK failures retry across process restarts. Ordinary JPEGs are atomically saved too. `maxEvidenceBytes` bounds on-disk evidence with whole-package reservations; completed, application-ACKed packages and ordinary JPEGs expire after `retentionDays`. Pending/failed ACKs and partial packages are retained; failed or abandoned packages require operator inspection. Expired packages cannot be acknowledged through REST. Retention removes completed inbox/outbox/jobs, historical events, and old closed lifecycle rows, never pending work. Alarm and evidence event history each keep at most `10 * maxInboxRows` newest records; deduplication is bounded by both that capacity and `retentionDays`. Do not manually alter the database while the service is running.

Build the service container from repository root:

```sh
docker build -f java/Dockerfile.service -t vdm-java-service .
# Supply a broker-reachable config, VDM_API_TOKEN, and a persistent /data volume.
```

The original CLI jar and classpath entries remain available as `target/vdm-mqtt-consumer-1.0.0.jar`. Minimal SDK-only recipes print concrete requests by default:

```sh
java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.BusinessExamples device
java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.BusinessExamples alarms
```

Recipes cover all five business areas. Replace sample IDs before adding `--execute`; the targets recipe is a deletion. Existing Consumer, ReceiveData, AlarmRpc, AlarmNotificationConsumer and EvidenceReceiver entries are unchanged. Their standalone lifecycle behavior differs from this durable service; use the service when restart recovery and broker ACK guarantees are required.

Deployment limits: one service process per data directory; use a private broker network or a TLS reverse proxy/tunnel because the SDK transport here is TCP MQTT. Use TLS termination for externally exposed HTTP. Protect and back up the data directory. These tests use local fixtures and simulated device responses; validate capability and parameter compatibility on your deployed firmware before production use.
