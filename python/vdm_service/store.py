"""Single-process SQLite WAL repository with explicit durable transactions."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import sqlite3
import threading
import time


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def identity(*parts):
    return hashlib.sha256(json.dumps(list(parts), ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


class Store:
    def __init__(self, settings):
        self.settings = settings
        settings.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._file_lock = (settings.data_dir / "service.lock").open("a+")
        try:
            fcntl.flock(self._file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._file_lock.close()
            raise RuntimeError("data directory already has a running service") from None
        self.lock = threading.RLock()
        self.closed = False
        try:
            self.db = sqlite3.connect(settings.data_dir / "service.sqlite3", check_same_thread=False, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA busy_timeout=5000")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f"unsupported database schema {version}")
            if version == 0:
                self.db.executescript('''
                BEGIN IMMEDIATE;
                CREATE TABLE inbox(id INTEGER PRIMARY KEY, connection_id TEXT, device_id TEXT,
                    topic TEXT, payload BLOB, status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
                    due REAL DEFAULT 0, created REAL, error TEXT);
                CREATE INDEX inbox_due ON inbox(status,due,id);
                CREATE TABLE latest(connection_id TEXT, device_id TEXT, telemetry TEXT, attributes TEXT,
                    received_count INTEGER DEFAULT 0, PRIMARY KEY(connection_id,device_id));
                CREATE TABLE alarm_events(connection_id TEXT,device_id TEXT,event_id TEXT,alarm_id TEXT,
                    payload TEXT,created REAL,PRIMARY KEY(connection_id,device_id,event_id));
                CREATE TABLE alarms(connection_id TEXT,device_id TEXT,alarm_id TEXT,ts INTEGER,
                    payload TEXT,needs_reconcile INTEGER DEFAULT 0,PRIMARY KEY(connection_id,device_id,alarm_id));
                CREATE TABLE outbox(key TEXT PRIMARY KEY,connection_id TEXT,device_id TEXT,payload TEXT,
                    status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,due REAL DEFAULT 0,created REAL,error TEXT);
                CREATE TABLE jobs(key TEXT PRIMARY KEY,connection_id TEXT,device_id TEXT,
                    status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,due REAL DEFAULT 0,created REAL,error TEXT);
                CREATE TABLE evidence_events(key TEXT PRIMARY KEY,connection_id TEXT,device_id TEXT,payload TEXT,created REAL);
                CREATE TABLE evidence(key TEXT PRIMARY KEY,connection_id TEXT,device_id TEXT,event_id TEXT,sha TEXT,
                    length INTEGER,path TEXT,status TEXT DEFAULT 'assembling',attempts INTEGER DEFAULT 0,
                    due REAL DEFAULT 0,created REAL,error TEXT);
                CREATE TABLE chunks(package_key TEXT,chunk_index INTEGER,topic TEXT,payload BLOB,
                    PRIMARY KEY(package_key,chunk_index));
                CREATE TABLE images(key TEXT PRIMARY KEY,path TEXT,length INTEGER,created REAL);
                CREATE TABLE counters(name TEXT PRIMARY KEY,value INTEGER DEFAULT 0);
                PRAGMA user_version=1;
                COMMIT;
                ''')
        except Exception:
            if hasattr(self, "db"):
                self.db.close()
            self._file_lock.close()
            raise

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def increment(self, name, amount=1):
        with self.transaction() as db:
            db.execute("INSERT INTO counters VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=value+excluded.value", (name, amount))

    def enqueue(self, cid, did, topic, payload):
        # Each record has a hard byte bound as well as the configurable row bound.
        limit = (min(32 * 1024 * 1024 + 76, self.settings.max_evidence_bytes)
                 if topic.endswith("/image") else 1024 * 1024)
        if len(payload) > limit:
            raise BufferError("MQTT payload exceeds bounded inbox record limit")
        with self.transaction() as db:
            count = db.execute("SELECT count(*) FROM inbox").fetchone()[0]
            if count >= self.settings.max_inbox_rows:
                # Processed records are only a replay aid; pending and failed work
                # is never evicted merely to admit newer traffic.
                db.execute("DELETE FROM inbox WHERE id IN (SELECT id FROM inbox WHERE status='done' ORDER BY id LIMIT ?)",
                           (count - self.settings.max_inbox_rows + 1,))
                count = db.execute("SELECT count(*) FROM inbox").fetchone()[0]
            if count >= self.settings.max_inbox_rows:
                raise BufferError("durable MQTT inbox is full")
            db.execute("INSERT INTO inbox(connection_id,device_id,topic,payload,created) VALUES(?,?,?,?,?)", (cid,did,topic,payload,time.time()))

    def next_pending(self, table, status="pending"):
        if table not in ("inbox", "outbox", "jobs", "evidence"):
            raise ValueError("invalid work table")
        with self.lock:
            return self.db.execute(f"SELECT * FROM {table} WHERE status=? AND due<=? ORDER BY created LIMIT 1", (status,time.time())).fetchone()

    def fail(self, table, row, exc):
        if table not in ("inbox", "outbox", "jobs", "evidence"):
            raise ValueError("invalid work table")
        attempts = row["attempts"] + 1
        field = "id" if table == "inbox" else "key"
        with self.transaction() as db:
            # Do not persist arbitrary exception text (URLs/credentials can occur).
            db.execute(f"UPDATE {table} SET status=?,attempts=?,due=?,error=? WHERE {field}=?",
                       ("failed" if attempts >= 8 else row["status"], attempts,
                        time.time()+min(300,2**attempts),type(exc).__name__,row[field]))

    def latest(self, cid, did):
        with self.lock:
            row = self.db.execute("SELECT * FROM latest WHERE connection_id=? AND device_id=?",(cid,did)).fetchone()
            return {"telemetry": json.loads(row["telemetry"]) if row and row["telemetry"] else None,
                    "attributes": json.loads(row["attributes"]) if row and row["attributes"] else None,
                    "receivedCount": row["received_count"] if row else 0}

    def alarms(self, cid, did):
        with self.lock:
            return [{**json.loads(row["payload"]), "needsReconcile":bool(row["needs_reconcile"])}
                    for row in self.db.execute("SELECT * FROM alarms WHERE connection_id=? AND device_id=? ORDER BY alarm_id",(cid,did))]

    def count(self, table):
        if table not in {"inbox","alarm_events","outbox","jobs","evidence_events","evidence","chunks","images"}:
            raise ValueError("invalid table")
        with self.lock:
            return self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def counters(self):
        with self.lock:
            result = dict(self.db.execute("SELECT name,value FROM counters"))
            for table in ("inbox","outbox","jobs","evidence"):
                for row in self.db.execute(f"SELECT status,count(*) FROM {table} GROUP BY status"):
                    result[f"{table}_{row[0]}"] = row[1]
            return result

    def prune(self):
        cutoff = time.time() - self.settings.retention_days * 86400
        with self.transaction() as db:
            db.execute("DELETE FROM inbox WHERE status='done' AND created<?", (cutoff,))
            db.execute("DELETE FROM alarm_events WHERE created<?", (cutoff,))
            db.execute("DELETE FROM evidence_events WHERE created<?", (cutoff,))
            db.execute("DELETE FROM outbox WHERE status='done' AND created<?", (cutoff,))
            db.execute("DELETE FROM jobs WHERE status='done' AND created<?", (cutoff,))
            # Closed lifecycle rows have their own retention rule. Historical
            # terminal events cannot retire a snapshot that says still active,
            # and unresolved ordering ambiguity is never pruned.
            for row in db.execute('SELECT rowid,ts,payload FROM alarms WHERE needs_reconcile=0').fetchall():
                state = json.loads(row['payload'])
                if state.get('currentActive') is True:
                    continue
                terminal_old = state.get('transition') in ('RECOVERED','CANCELLED') and row['ts'] < cutoff
                reconciled_at = state.get('reconciledAt')
                reconciled_closed_old = (state.get('currentActive') is False
                                         and isinstance(reconciled_at,(int,float))
                                         and reconciled_at < cutoff)
                if terminal_old or reconciled_closed_old:
                    db.execute('DELETE FROM alarms WHERE rowid=?',(row['rowid'],))
            # Processed history has an independent row budget; incident state and
            # every pending/failed outbox/task remain intact when history ages out.
            history_limit = 10 * self.settings.max_inbox_rows
            for table in ('alarm_events', 'evidence_events'):
                db.execute(f"DELETE FROM {table} WHERE rowid IN "
                           f"(SELECT rowid FROM {table} ORDER BY created DESC,rowid DESC LIMIT -1 OFFSET ?)",
                           (history_limit,))
        # File/receipt retention runs through EvidenceService under its assembler
        # lock before this metadata-only pruning (see Service.prune).
        with self.lock:
            self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def close(self):
        with self.lock:
            if not self.closed:
                self.closed = True
                self.db.close()
                self._file_lock.close()
