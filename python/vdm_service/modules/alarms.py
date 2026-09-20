"""Alarm lifecycle ordering, deduplication and atomic notification decisions."""
import json
import time
from ..store import dumps, identity

TRANSITIONS = {"TRIGGERED","ESCALATED","DEESCALATED","SYNCED","RECOVERED","CANCELLED"}


def process(db, cid, did, event, max_age, max_pending_rows):
    event = dict(event)
    for key in ("eventId", "alarmId"):
        text = str(event.get(key, ""))
        if not text.isdecimal() or not 0 < int(text) < 2**64:
            raise ValueError(f"invalid {key}")
        event[key] = str(int(text))
    transition = str(event.get("transition", "")).removeprefix("ALARM_TRANSITION_")
    if transition not in TRANSITIONS:
        raise ValueError("invalid alarm transition")
    event["transition"] = transition
    if "level" in event:
        event["level"] = str(event["level"]).removeprefix("ALARM_LEVEL_")
    raw_ts = event.get("ts")
    if isinstance(raw_ts, bool) or not str(raw_ts).isdecimal():
        raise ValueError("alarm ts must be Unix seconds")
    ts = int(raw_ts)
    if ts > 2**63-1:
        raise ValueError("alarm ts too large")
    event["ts"] = ts
    now = time.time()
    if not db.execute("INSERT OR IGNORE INTO alarm_events VALUES(?,?,?,?,?,?)",
                      (cid,did,event["eventId"],event["alarmId"],dumps(event),now)).rowcount:
        return
    previous = db.execute("SELECT * FROM alarms WHERE connection_id=? AND device_id=? AND alarm_id=?",(cid,did,event["alarmId"])).fetchone()
    if previous and ts <= previous["ts"]:
        job_key = identity(cid,did,"reconcile")
        existing_job = db.execute('SELECT status FROM jobs WHERE key=?',(job_key,)).fetchone()
        if existing_job is None or existing_job['status'] == 'done':
            if db.execute("SELECT count(*) FROM jobs WHERE status<>'done'").fetchone()[0] >= max_pending_rows:
                raise BufferError('reconciliation job queue is full')
        db.execute("UPDATE alarms SET needs_reconcile=1 WHERE connection_id=? AND device_id=? AND alarm_id=?",(cid,did,event["alarmId"]))
        db.execute("INSERT INTO jobs(key,connection_id,device_id,created) VALUES(?,?,?,?) ON CONFLICT(key) DO UPDATE SET status='pending',attempts=0,due=0,created=excluded.created",(job_key,cid,did,now))
        return
    event["status"] = "CLOSED" if transition in {"RECOVERED","CANCELLED"} else "ACTIVE"
    event["currentActive"] = event["status"] == "ACTIVE"
    if event["status"] == "CLOSED":
        event.pop("level", None)
    # An unresolved ambiguity stays marked even when a newer event arrives.
    db.execute("INSERT INTO alarms VALUES(?,?,?,?,?,0) ON CONFLICT(connection_id,device_id,alarm_id) DO UPDATE SET ts=excluded.ts,payload=excluded.payload",
               (cid,did,event["alarmId"],ts,dumps(event)))
    prior = json.loads(previous['payload']) if previous else None
    levels = {'ALERT':1,'ALARM':2,'ACTION':3}
    previous_level = (prior.get('currentLevel', prior.get('level')) if prior else None)
    previous_rank = levels.get(str(previous_level).removeprefix('ALARM_LEVEL_'),0)
    current_rank = levels.get(event.get('level'),0)
    actionable = (transition in {'RECOVERED','CANCELLED'}
                  or transition == 'TRIGGERED' and previous is None
                  or transition == 'ESCALATED' and (previous is None or current_rank > previous_rank))
    if actionable and 0 <= now-ts <= max_age:
        if db.execute("SELECT count(*) FROM outbox WHERE status<>'done'").fetchone()[0] >= max_pending_rows:
            raise BufferError("notification outbox is full")
        key = identity(cid,did,event["eventId"],transition)
        payload = dict(connectionId=cid,deviceId=did,eventId=event["eventId"],transition=transition,alarm=event)
        db.execute("INSERT OR IGNORE INTO outbox(key,connection_id,device_id,payload,created) VALUES(?,?,?,?,?)",(key,cid,did,dumps(payload),now))


def reconcile(db, cid, did, response):
    body = response.get("getAlarmState", response.get("data", {}))
    if not isinstance(body, dict) or not isinstance(body.get("active", []), list):
        raise ValueError("invalid getAlarmState response")
    active = {str(item["alarmId"]): item for item in body.get("active", [])}
    for row in db.execute("SELECT * FROM alarms WHERE connection_id=? AND device_id=?",(cid,did)).fetchall():
        event = json.loads(row["payload"])
        item = active.pop(row["alarm_id"], None)
        event["status"] = "ACTIVE" if item else "CLOSED"
        event["currentActive"] = item is not None
        event["currentState"] = item
        event["reconciledAt"] = int(time.time())
        event["currentLevel"] = str(item["level"]).removeprefix("ALARM_LEVEL_") if item and "level" in item else None
        # Keep original eventId/transition/ts/level as historical evidence.
        # The device snapshot only changes the separate current lifecycle view.
        db.execute("UPDATE alarms SET payload=?,needs_reconcile=0 WHERE connection_id=? AND device_id=? AND alarm_id=?", (dumps(event),cid,did,row["alarm_id"]))
    for aid, item in active.items():
        event = {**item,"status":"ACTIVE","currentActive":True,"currentState":item,"reconciledAt":int(time.time()),"transition":"SYNCED","ts":0}
        if "level" in event:
            event["level"] = str(event["level"]).removeprefix("ALARM_LEVEL_")
        db.execute("INSERT INTO alarms VALUES(?,?,?,?,?,0)",(cid,did,aid,0,dumps(event)))
