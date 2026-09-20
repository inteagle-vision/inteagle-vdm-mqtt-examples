"""Latest telemetry persistence. Historical synchronization uses the RPC catalog."""
from ..store import dumps


def process(db, cid, did, payload):
    db.execute("INSERT INTO latest(connection_id,device_id,telemetry,received_count) VALUES(?,?,?,1) ON CONFLICT(connection_id,device_id) DO UPDATE SET telemetry=excluded.telemetry,received_count=received_count+1", (cid,did,dumps(payload)))
