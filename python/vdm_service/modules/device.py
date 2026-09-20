"""Device metadata and the most recently received attribute report."""
from ..store import dumps


def process(db, cid, did, payload):
    db.execute("INSERT INTO latest(connection_id,device_id,attributes,received_count) VALUES(?,?,?,1) ON CONFLICT(connection_id,device_id) DO UPDATE SET attributes=excluded.attributes,received_count=received_count+1", (cid,did,dumps(payload)))


def public_metadata(device):
    return {key:device[key] for key in ("connectionId","deviceId","format","capabilities")}
