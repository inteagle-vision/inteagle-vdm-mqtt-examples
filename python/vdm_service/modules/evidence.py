"""Disk-first evidence verification with replayable chunks and durable ACK jobs."""
import hashlib
import os
from pathlib import Path
import time
import threading
from vdm_mqtt_sdk import AlarmSnapshotPackageAssembler, EvidencePackageChunk, ImageFrame
from ..store import dumps, identity


def process_event(db, cid, did, payload):
    text = dumps(payload)
    db.execute("INSERT OR IGNORE INTO evidence_events VALUES(?,?,?,?,?)", (identity(cid,did,text),cid,did,text,time.time()))


class EvidenceService:
    def __init__(self, store, settings):
        self.store = store
        self.settings = settings
        self.root = settings.data_dir / "evidence"
        self.root.mkdir(exist_ok=True,mode=0o700)
        # Serialize assembler IO, verification and retention of the same files.
        self.lock = threading.RLock()

    def _sync_parents(self, path):
        # Sync every newly-created directory link, not only the event directory.
        # Otherwise a power failure can lose an ancestor after a receipt fsync.
        path = path.resolve()
        boundary = self.settings.data_dir.resolve()
        while True:
            AlarmSnapshotPackageAssembler._fsync_directory(path)
            if path == boundary or path == path.parent:
                break
            path = path.parent

    def _reserve(self, db, amount):
        # Reserve both the replay ledger and assembled package. Include ordinary
        # images and untracked files left after a crash conservatively.
        reserved = db.execute("SELECT coalesce(sum(length*2),0) FROM evidence").fetchone()[0]
        reserved += db.execute("SELECT coalesce(sum(length),0) FROM images").fetchone()[0]
        disk = sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file())
        if max(reserved,disk) + amount > self.settings.max_evidence_bytes:
            raise BufferError("evidence capacity exhausted")

    def process(self, cid, did, decoded):
        with self.lock:
            return self._process(cid, did, decoded)

    def _process(self, cid, did, decoded):
        value = decoded.value
        if isinstance(value, ImageFrame):
            key = identity(cid,did,hashlib.sha256(decoded.raw).hexdigest())
            path = self.root / cid / did / "images" / f"{key}.jpg"
            with self.store.transaction() as db:
                if db.execute("SELECT 1 FROM images WHERE key=?",(key,)).fetchone():
                    return
                self._reserve(db, len(value.jpeg))
                path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                temporary = path.with_suffix('.tmp')
                with temporary.open('wb') as output:
                    output.write(value.jpeg); output.flush(); os.fsync(output.fileno())
                os.replace(temporary,path)
                self._sync_parents(path.parent)
                db.execute("INSERT INTO images VALUES(?,?,?,?)",(key,str(path),len(value.jpeg),time.time()))
            return
        if not isinstance(value, EvidencePackageChunk):
            raise ValueError("unsupported image payload")
        key = identity(cid,did,str(value.event_id),value.package_sha256.hex())
        # Persist a chunk ledger before touching assembler state. On every attempt
        # replay this bounded package's ledger, so no in-memory bitmap is required.
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM evidence WHERE key=?",(key,)).fetchone()
            if row is None:
                conflict = db.execute("SELECT 1 FROM evidence WHERE connection_id=? AND device_id=? AND event_id=? AND sha<>?",(cid,did,str(value.event_id),value.package_sha256.hex())).fetchone()
                if conflict:
                    raise ValueError("event has conflicting evidence package hash")
                self._reserve(db, value.package_length*2 + 4096)
                db.execute("INSERT INTO evidence(key,connection_id,device_id,event_id,sha,length,created) VALUES(?,?,?,?,?,?,?)",(key,cid,did,str(value.event_id),value.package_sha256.hex(),value.package_length,time.time()))
            old = db.execute("SELECT payload FROM chunks WHERE package_key=? AND chunk_index=?",(key,value.chunk_index)).fetchone()
            if old and bytes(old[0]) != decoded.raw:
                raise ValueError("conflicting duplicate evidence chunk")
            db.execute("INSERT OR IGNORE INTO chunks VALUES(?,?,?,?)",(key,value.chunk_index,decoded.topic,decoded.raw))
            chunks = db.execute("SELECT payload FROM chunks WHERE package_key=? ORDER BY chunk_index",(key,)).fetchall()
        assembler = AlarmSnapshotPackageAssembler(self.root / cid / did / "packages")
        completed = None
        from vdm_mqtt_sdk import VdmCodec
        for chunk in chunks:
            completed = assembler.accept(VdmCodec.decode_image(bytes(chunk[0]))) or completed
        if completed:
            self._sync_parents(completed.package_path.parent)
            # accept() fsyncs the validated tar + receipt BEFORE this ACK job exists.
            with self.store.transaction() as db:
                db.execute("UPDATE evidence SET path=?,status=CASE WHEN status='assembling' THEN 'pending' ELSE status END WHERE key=?",(str(completed.package_path),key))
                db.execute("DELETE FROM chunks WHERE package_key=?",(key,))

    def verified(self, cid, did, event_id, sha):
        with self.lock:
            return self._verified(cid, did, event_id, sha)

    def _verified(self, cid, did, event_id, sha):
        with self.store.lock:
            row = self.store.db.execute("SELECT * FROM evidence WHERE connection_id=? AND device_id=? AND event_id=? AND sha=? AND status<>'assembling'",(cid,did,str(event_id),sha)).fetchone()
        if not row or not row['path']:
            return False
        try:
            AlarmSnapshotPackageAssembler._validate_complete_package(Path(row['path']),row['length'],bytes.fromhex(row['sha']))
        except (OSError,ValueError):
            return False
        return True


    def _safe_retention_path(self, path):
        """Accept only ordinary files under the configured root, without links."""
        root = self.root.absolute()
        candidate = Path(path).absolute()
        try:
            relative = candidate.relative_to(root)
        except ValueError:
            return False
        if not relative.parts or any(part in (".", "..") for part in relative.parts):
            return False
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                return False
        return candidate.resolve().is_relative_to(root.resolve()) and (
            not candidate.exists() or candidate.is_file())

    @staticmethod
    def _unsafe_retention_path(db):
        db.execute("INSERT INTO counters VALUES('evidenceRetentionUnsafePaths',1) "
                   "ON CONFLICT(name) DO UPDATE SET value=value+1")

    def prune(self):
        """Retire only old, application-ACKed packages and ordinary JPEG files.

        Delete files before their capacity/receipt rows. A crash between those
        steps leaves a retryable done row whose absent file cannot pass verified().
        Pending/failed acknowledgements and assembling chunk ledgers are excluded.
        """
        cutoff = time.time() - self.settings.retention_days * 86400
        with self.lock, self.store.transaction() as db:
            packages = db.execute("SELECT * FROM evidence WHERE status='done' AND created<?", (cutoff,)).fetchall()
            for row in packages:
                expected = (self.root / row['connection_id'] / row['device_id'] /
                            'packages' / row['event_id'] / (row['sha'] + '.tar'))
                receipt = expected.parent / 'receipt.json'
                paths = [expected, receipt, expected.with_suffix('.tar.part'),
                         expected.parent / 'receipt.json.tmp']
                if (not row['path'] or Path(row['path']).absolute() != expected.absolute()
                        or not all(self._safe_retention_path(path) for path in paths)):
                    self._unsafe_retention_path(db)
                    continue
                for path in paths:
                    path.unlink(missing_ok=True)
                self._sync_parents(expected.parent if expected.parent.is_dir() else self.root)
                db.execute('DELETE FROM chunks WHERE package_key=?', (row['key'],))
                db.execute('DELETE FROM evidence WHERE key=?', (row['key'],))
            images = db.execute('SELECT * FROM images WHERE created<?', (cutoff,)).fetchall()
            for row in images:
                path = Path(row['path'])
                try:
                    parts = path.absolute().relative_to(self.root.absolute()).parts
                except ValueError:
                    parts = ()
                if (len(parts) != 4 or parts[2] != 'images' or parts[3] != row['key'] + '.jpg'
                        or not self._safe_retention_path(path)):
                    self._unsafe_retention_path(db)
                    continue
                path.unlink(missing_ok=True)
                self._sync_parents(path.parent if path.parent.is_dir() else self.root)
                db.execute('DELETE FROM images WHERE key=?', (row['key'],))
