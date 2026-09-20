# Implementation ledger — VDM multilingual services

Approved source: user implementation plan 2026-09-20.
- Located intended reference repository adjacent to firmware workspace.
- Existing baseline: 29 SDK RPC methods, four data/evidence examples, Python and Java alarm notification reference logic.
- Protocol discrepancy: actual firmware has 33 public routes including listAlarmEvents and telemetry sync operations; copied authoritative wire schema without changing firmware.
- Execution: shared contract first, independent language implementations, common integration verification and documentation.
- Current phase: all software stages implemented and locally verified; real-device
  MQTT acceptance remains pending broker reachability. See ../acceptance.md.

## Integration evidence
- Final local suites: Python 63, Go 145 including subtests, Java 54,
  JavaScript 45; Go race/vet and JS typecheck passed. All service images and
  authenticated Compose startup passed. Legacy JSON/Protobuf smoke passed.
- Temporary acceptance containers, brokers and volumes were removed after testing.
- All four services passed the same real-MQTT/two-broker HTTP smoke.
- Added shared 33-method JSON requests and exact Protobuf wire fixtures.
- Expanded smoke covers equal-time reconciliation, webhook failed delivery retry
  with stable idempotency key, out-of-order partial image across service restart,
  and verified evidence ACK retry; all four passed on fresh test brokers.
- Independent review reproduced JS total RPC deadline, empty ProtoJSON state,
  large JPEG ingress, active-history retention, and failed-reconciliation rearm.
  Each received a failing regression and fix. Further cross-language review aligned
  empty request bodies, malformed trailing JSON rejection, lifecycle notification
  policy, history/outbox capacity and evidence retention.
- Ruling: implementation belongs to adjacent examples repository; firmware was
  read only. No real credentials/device addresses committed.
- Ruling: use actual firmware's 33 public RPCs rather than stale SDK list of29;
  preserved8 documented-but-unavailable RPCs as rejected, no invented capability.
- Ruling: first-trigger and increasing-level notification semantics follow user plan;
  local reconciled state is separate from immutable historical event timestamps.
- HIL: authorized SSH endpoint reachable, device process running. Configured standard
  MQTT broker connection refused from workstation; no firmware/config/measurement
  mutations performed. MQTT hardware validation remains unverified.
