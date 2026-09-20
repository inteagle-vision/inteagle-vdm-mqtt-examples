import hashlib
import io
import json
import tarfile
import unittest
from fastapi.testclient import TestClient
import inteagle_vdm_mqtt_v1_pb2 as pb
from vdm_mqtt_sdk import DecodedPayload
from vdm_service.app import create_app, OPERATIONS
from vdm_service.config import Settings
from vdm_service.runtime import Service
import test_service as base


class FailureTests(base.ServiceFixture):
    def test_outbox_backlog_is_bounded_without_dropping_alarm(self):
        self.svc.settings.max_inbox_rows=1
        self.ingest('3A',self.alarm('1'))
        self.ingest('3A',{**self.alarm('2'),'alarmId':'100'})
        self.assertEqual(self.svc.store.count('outbox'),1)
        self.assertEqual(self.svc.store.count('alarm_events'),1)
        self.assertEqual(self.svc.store.counters()['inbox_pending'],1)

    def test_reconciliation_preserves_original_event_fields(self):
        event=self.alarm('1')
        self.ingest('3A',event)
        self.ingest('3A',self.alarm('2',event['ts'],'RECOVERED'))
        self.svc.process_job()
        local=self.svc.store.alarms('local','DEMO')[0]
        self.assertEqual(local['transition'],'TRIGGERED')
        self.assertFalse(local['currentActive'])
        self.assertFalse(local['needsReconcile'])
        self.assertIsNone(local['currentState'])
        self.assertIsInstance(local['reconciledAt'],int)

    def test_non_ascii_authorization_is_rejected_without_server_error(self):
        self.svc.settings.api_token='secret'
        client=TestClient(create_app(self.svc,manage_lifecycle=False),raise_server_exceptions=False)
        response=client.get('/v1/devices',headers={b'Authorization':b'Bearer \xff'})
        self.assertEqual(response.status_code,401)

    def test_malformed_rpc_transport_error_requests_reconnect(self):
        self.svc.transport_error('local','DEMO',ValueError('invalid rpc payload'))
        self.assertIn(('local','DEMO'),self.svc.reconnect)

    def test_distinct_repeated_triggers_notify_only_first_lifecycle(self):
        import time
        now=int(time.time())
        self.ingest('3A',self.alarm('1',now-2))
        self.ingest('3A',self.alarm('2',now-1))
        self.assertEqual(self.svc.store.count('alarm_events'),2)
        self.assertEqual(self.svc.store.count('outbox'),1)
        self.assertEqual(self.svc.store.alarms('local','DEMO')[0]['eventId'],'2')

    def test_escalation_notifies_only_for_increased_level(self):
        import time
        now=int(time.time())
        self.ingest('3A',self.alarm('1',now-4))
        self.ingest('3A',self.alarm('2',now-3,'ESCALATED'))
        self.assertEqual(self.svc.store.count('outbox'),1)
        self.ingest('3A',{**self.alarm('3',now-2,'ESCALATED'),'level':'ALARM'})
        self.assertEqual(self.svc.store.count('outbox'),2)
        self.ingest('3A',{**self.alarm('4',now-1,'ESCALATED'),'level':'ACTION'})
        self.assertEqual(self.svc.store.count('outbox'),3)

    def test_history_capacity_keeps_lifecycle_and_pending_work(self):
        import time
        self.svc.settings.max_inbox_rows=1
        now=int(time.time())
        for index in range(12):
            self.ingest('3A',self.alarm(str(index+1),now-20+index,'SYNCED'))
            self.ingest('event',{'eventId':str(index+1),'state':'PENDING'})
        with self.svc.store.transaction() as db:
            db.execute("INSERT INTO outbox(key,connection_id,device_id,payload,created) VALUES('keep','local','DEMO','{}',0)")
        self.svc.prune()
        self.assertEqual(self.svc.store.count('alarm_events'),10)
        self.assertEqual(self.svc.store.count('evidence_events'),10)
        self.assertEqual(self.svc.store.alarms('local','DEMO')[0]['eventId'],'12')
        self.assertEqual(self.svc.store.count('outbox'),1)
        remaining={row[0] for row in self.svc.store.db.execute('SELECT event_id FROM alarm_events')}
        self.assertNotIn('1',remaining)
        self.assertNotIn('2',remaining)

    def test_protobuf_zero_code_is_success(self):
        self.svc.devices[('local','DEMO')]['format']='protobuf'
        response=pb.RpcResponse(schema_version=1,req_id=1)
        response.get_attr.SetInParent()
        self.svc.clients[('local','DEMO')].call=lambda *a,**k:DecodedPayload('vdm/DEMO/rpc/resp','rpc/resp',response.SerializeToString(),response)
        result=TestClient(create_app(self.svc,manage_lifecycle=False)).post('/v1/connections/local/devices/DEMO/device/attributes/query',json={'params':{'keys':['deviceId']}})
        self.assertEqual(result.status_code,200,result.text)

    def test_webhook_failure_retry_uses_same_key_after_restart(self):
        self.svc.settings.webhook_url='http://localhost/callback'
        self.ingest('3A',self.alarm('9007199254740993'))
        delivered=[]
        def unavailable(url,payload,headers):
            delivered.append(headers['Idempotency-Key'])
            raise OSError('temporarily unavailable')
        self.svc.webhook_sender=unavailable
        self.svc.process_outbox()
        self.assertEqual(self.svc.store.counters()['outbox_pending'],1)
        self.svc.close()
        self.svc=Service(self.settings,client_factory=base.Device,webhook_sender=lambda u,p,h:delivered.append(h['Idempotency-Key']))
        self.addCleanup(self.svc.close)
        with self.svc.store.transaction() as db:
            db.execute('UPDATE outbox SET due=0')
        self.svc.process_outbox()
        self.assertEqual(delivered[0],delivered[1])
        self.assertEqual(self.svc.store.counters()['outbox_done'],1)

    def test_webhook_attempts_are_bounded_and_reported(self):
        self.svc.settings.webhook_url='http://localhost/callback'
        self.ingest('3A',self.alarm())
        def unavailable(*args): raise TimeoutError()
        self.svc.webhook_sender=unavailable
        for _ in range(8):
            with self.svc.store.transaction() as db: db.execute('UPDATE outbox SET due=0')
            self.svc.process_outbox()
        self.assertFalse(self.svc.process_outbox())
        self.assertEqual(self.svc.health()['status'],'degraded')
        self.assertEqual(self.svc.store.counters()['outbox_failed'],1)

    def test_retention_never_discards_pending_or_failed_work(self):
        self.ingest('3A',self.alarm())
        with self.svc.store.transaction() as db:
            db.execute('UPDATE inbox SET created=0')
            db.execute('UPDATE outbox SET created=0')
        self.svc.store.prune()
        self.assertEqual(self.svc.store.count('inbox'),0)
        self.assertEqual(self.svc.store.count('outbox'),1)

    def test_alarm_transaction_rolls_back_on_outbox_failure(self):
        with self.svc.store.transaction() as db:
            db.execute("CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'failed'); END")
        self.ingest('3A',self.alarm())
        self.assertEqual(self.svc.store.count('alarm_events'),0)
        self.assertEqual(self.svc.store.alarms('local','DEMO'),[])
        self.assertEqual(self.svc.store.counters()['inbox_pending'],1)

    def test_all_routes_registered_once_and_no_raw_rpc(self):
        app=create_app(self.svc,manage_lifecycle=False)
        paths={route.path for route in app.routes}
        self.assertEqual(len(OPERATIONS),33)
        for operation in OPERATIONS:
            self.assertIn('/v1/connections/{connectionId}/devices/{deviceId}/'+operation['route'],paths)
        client=TestClient(app)
        prefix='/v1/connections/local/devices/DEMO/'
        self.assertEqual(client.post(prefix+'rpc',json={}).status_code,404)
        self.assertEqual(client.post(prefix+'targets/add',content='x'*1048577).status_code,400)
        self.assertEqual(client.post(prefix+'targets/add',content='{"params":{"value":NaN}}').status_code,400)

    def test_connection_device_namespaces_do_not_cross(self):
        self.svc.close()
        config=json.loads(json.dumps(base.CONFIG))
        config['connections'].append({**config['connections'][0],'id':'other'})
        self.settings=Settings.from_dict(config,data_dir=self.tmp.name,environ={})
        self.svc=Service(self.settings,client_factory=base.Device)
        self.addCleanup(self.svc.close)
        self.ingest('3A',self.alarm('1'))
        self.ingest('3A',self.alarm('1'),connection='other')
        self.assertEqual(self.svc.store.count('alarm_events'),2)
        self.assertEqual(self.svc.store.count('outbox'),2)
        self.assertNotEqual(self.svc.clients[('local','DEMO')].config.client_id,self.svc.clients[('other','DEMO')].config.client_id)

    @staticmethod
    def package():
        output=io.BytesIO()
        with tarfile.open(fileobj=output,mode='w',format=tarfile.USTAR_FORMAT) as tar:
            for name,data in [('manifest.json',b'{"eventId":"42"}'),('frame.jpg',b'\xff\xd8'+b'x'*140000+b'\xff\xd9')]:
                info=tarfile.TarInfo(name); info.size=len(data)
                tar.addfile(info,io.BytesIO(data))
        return output.getvalue()

    @staticmethod
    def chunk(package,index):
        size=128*1024
        part=package[index*size:(index+1)*size]
        return (bytes([2,76,1,1])+(42).to_bytes(8,'big')+len(package).to_bytes(8,'big')+
                hashlib.sha256(package).digest()+index.to_bytes(4,'big')+
                ((len(package)+size-1)//size).to_bytes(4,'big')+(index*size).to_bytes(8,'big')+
                len(part).to_bytes(4,'big')+bytes(4)+part)

    def test_evidence_restart_reassembles_then_receipt_before_ack_retry(self):
        package=self.package()
        self.svc.ingest('local','DEMO','vdm/DEMO/image',self.chunk(package,0))
        self.svc.process_one()
        self.assertFalse(self.svc.process_ack())
        self.svc.close()
        self.svc=Service(self.settings,client_factory=base.Device)
        self.addCleanup(self.svc.close)
        self.svc.ingest('local','DEMO','vdm/DEMO/image',self.chunk(package,1))
        self.svc.process_one()
        self.assertEqual(self.svc.store.counters()['evidence_pending'],1)
        self.assertTrue(self.svc.evidence.verified('local','DEMO','42',hashlib.sha256(package).hexdigest()))
        self.svc.clients[('local','DEMO')].error=TimeoutError()
        self.svc.process_ack()
        self.assertEqual(self.svc.store.counters()['evidence_pending'],1)
        self.svc.close()
        self.svc=Service(self.settings,client_factory=base.Device)
        self.addCleanup(self.svc.close)
        with self.svc.store.transaction() as db: db.execute('UPDATE evidence SET due=0')
        self.svc.process_ack()
        self.assertEqual(self.svc.store.counters()['evidence_done'],1)
        call=self.svc.clients[('local','DEMO')].calls[0]
        self.assertEqual(call,('ackEvidencePackage',{'eventId':'42','kind':'SNAPSHOT','packageSha256':hashlib.sha256(package).hexdigest()}))

    def test_evidence_directory_sync_failure_cannot_make_ack_eligible(self):
        from unittest.mock import patch
        from vdm_mqtt_sdk import AlarmSnapshotPackageAssembler
        package=self.package()
        sync=AlarmSnapshotPackageAssembler._fsync_directory
        def sync_or_fail(path):
            if path == self.svc.evidence.root.resolve():
                raise OSError('directory durability failed')
            return sync(path)
        with patch.object(AlarmSnapshotPackageAssembler,'_fsync_directory',side_effect=sync_or_fail):
            for index in (0,1):
                self.svc.ingest('local','DEMO','vdm/DEMO/image',self.chunk(package,index))
                self.svc.process_one()
        self.assertFalse(self.svc.process_ack())
        self.assertEqual(self.svc.store.counters()['evidence_assembling'],1)
        self.assertEqual(self.svc.store.count('chunks'),2)

    def test_two_megabyte_ordinary_jpeg_is_saved_within_evidence_budget(self):
        self.svc.settings.max_evidence_bytes=4*1024*1024
        jpeg=b'\xff\xd8'+b'x'*(2*1024*1024)+b'\xff\xd9'
        raw=bytes([1,8,0,1])+(123).to_bytes(4,'big')+jpeg
        try:
            self.svc.ingest('local','DEMO','vdm/DEMO/image',raw)
        except BufferError as exc:
            self.fail(f'valid 2MiB JPEG rejected before persistence: {exc}')
        self.svc.process_one()
        self.assertEqual(self.svc.store.count('images'),1)
        image_path=self.svc.store.db.execute('SELECT path FROM images').fetchone()[0]
        from pathlib import Path
        self.assertEqual(Path(image_path).read_bytes(),jpeg)
        with self.assertRaises(BufferError):
            self.svc.ingest('local','DEMO','vdm/DEMO/telemetry',b'x'*(1024*1024+1))

    def test_evidence_capacity_never_creates_ack(self):
        self.svc.settings.max_evidence_bytes=200000
        self.svc.ingest('local','DEMO','vdm/DEMO/image',self.chunk(self.package(),0))
        self.svc.process_one()
        self.assertEqual(self.svc.store.count('evidence'),0)
        self.assertFalse(self.svc.process_ack())
        self.assertEqual(self.svc.store.counters()['inbox_pending'],1)

    def test_unverified_explicit_evidence_ack_is_rejected(self):
        response=TestClient(create_app(self.svc,manage_lifecycle=False)).post('/v1/connections/local/devices/DEMO/evidence/ack',json={'params':{'eventId':'42','kind':'SNAPSHOT','packageSha256':'f'*64}})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.svc.clients[('local','DEMO')].calls,[])

if __name__=='__main__':unittest.main()
