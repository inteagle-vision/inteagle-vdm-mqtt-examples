"""Retention must release disk capacity without losing unacknowledged evidence."""
from pathlib import Path
import tempfile
import unittest
import test_service as base
import test_service_failures as fixtures


class EvidenceRetentionTests(base.ServiceFixture):
    def package(self, *, complete=True, ack=True):
        body=fixtures.FailureTests.package()
        for index in ((0,1) if complete else (0,)):
            self.svc.ingest('local','DEMO','vdm/DEMO/image',fixtures.FailureTests.chunk(body,index))
            self.svc.process_one()
        if complete and ack:
            self.svc.process_ack()
        return self.svc.store.db.execute('SELECT * FROM evidence').fetchone()

    def jpeg(self):
        self.svc.ingest('local','DEMO','vdm/DEMO/image',bytes([1,8,0,1])+bytes(4)+b'\xff\xd8picture\xff\xd9')
        self.svc.process_one()
        return self.svc.store.db.execute('SELECT * FROM images').fetchone()

    def prune(self):
        self.assertTrue(hasattr(self.svc,'prune'),'service does not coordinate evidence retention')
        self.svc.prune()

    def test_old_acked_package_and_jpeg_are_removed_and_cannot_be_acked_again(self):
        package=self.package()
        jpeg=self.jpeg()
        with self.svc.store.transaction() as db:
            db.execute('UPDATE evidence SET created=0')
            db.execute('UPDATE images SET created=0')
        receipt=Path(package['path']).parent/'receipt.json'
        self.assertTrue(receipt.is_file())
        self.prune()
        self.assertFalse(Path(package['path']).exists())
        self.assertFalse(receipt.exists())
        self.assertFalse(Path(jpeg['path']).exists())
        self.assertEqual(self.svc.store.count('evidence'),0)
        self.assertEqual(self.svc.store.count('images'),0)
        self.assertFalse(self.svc.evidence.verified('local','DEMO',package['event_id'],package['sha']))
        self.assertFalse(self.svc.process_ack())

    def test_pending_and_failed_ack_packages_are_retained(self):
        package=self.package(ack=False)
        for status in ('pending','failed'):
            with self.svc.store.transaction() as db:
                db.execute('UPDATE evidence SET created=0,status=?',(status,))
            self.prune()
            self.assertTrue(Path(package['path']).is_file())
            self.assertTrue((Path(package['path']).parent/'receipt.json').is_file())
            self.assertEqual(self.svc.store.count('evidence'),1)

    def test_partial_and_recent_completed_packages_are_retained(self):
        self.package(complete=False)
        with self.svc.store.transaction() as db: db.execute('UPDATE evidence SET created=0')
        self.prune()
        self.assertEqual(self.svc.store.count('chunks'),1)
        self.assertTrue(list(self.svc.evidence.root.rglob('*.part')))
        body=fixtures.FailureTests.package()
        self.svc.ingest('local','DEMO','vdm/DEMO/image',fixtures.FailureTests.chunk(body,1))
        self.svc.process_one()
        self.svc.process_ack()
        import time
        with self.svc.store.transaction() as db: db.execute('UPDATE evidence SET created=?',(time.time(),))
        self.prune()
        self.assertEqual(self.svc.store.count('evidence'),1)

    def test_retention_waits_for_inflight_duplicate_package_assembly(self):
        import threading
        from unittest.mock import patch
        from vdm_mqtt_sdk import AlarmSnapshotPackageAssembler
        package=self.package()
        with self.svc.store.transaction() as db: db.execute('UPDATE evidence SET created=0')
        body=fixtures.FailureTests.package()
        self.svc.ingest('local','DEMO','vdm/DEMO/image',fixtures.FailureTests.chunk(body,0))
        entered=threading.Event(); resume=threading.Event(); pruning=threading.Event(); pruned=threading.Event()
        accept=AlarmSnapshotPackageAssembler.accept
        def paused_accept(assembler,chunk):
            entered.set()
            if not resume.wait(3): raise TimeoutError('test did not release assembler')
            return accept(assembler,chunk)
        def prune():
            pruning.set()
            self.svc.prune()
            pruned.set()
        with patch.object(AlarmSnapshotPackageAssembler,'accept',paused_accept):
            processing=threading.Thread(target=self.svc.process_one)
            cleanup=threading.Thread(target=prune)
            processing.start()
            try:
                self.assertTrue(entered.wait(1))
                cleanup.start()
                self.assertTrue(pruning.wait(1))
                self.assertFalse(pruned.wait(.05))
                self.assertTrue(Path(package['path']).is_file())
            finally:
                resume.set()
                processing.join(3)
                if cleanup.ident is not None: cleanup.join(3)
        self.assertTrue(pruned.is_set())
        self.assertEqual(self.svc.store.count('evidence'),0)
        self.assertNotIn('inbox_failed',self.svc.store.counters())

    def test_retention_never_follows_a_symlink_outside_evidence_root(self):
        jpeg=self.jpeg()
        image_path=Path(jpeg['path'])
        with tempfile.TemporaryDirectory() as external:
            outside=Path(external)/'private.jpg'; outside.write_bytes(b'keep me')
            image_path.unlink(); image_path.symlink_to(outside)
            with self.svc.store.transaction() as db: db.execute('UPDATE images SET created=0')
            self.prune()
            self.assertEqual(outside.read_bytes(),b'keep me')
            self.assertEqual(self.svc.store.count('images'),1)
            self.assertGreater(self.svc.store.counters().get('evidenceRetentionUnsafePaths',0),0)

if __name__=='__main__':unittest.main()
