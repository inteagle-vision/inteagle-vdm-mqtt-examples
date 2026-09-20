"""Application orchestration. MQTT callbacks persist only; workers perform IO/RPC."""
import hashlib
import json
import logging
import threading
import time
import httpx
from vdm_mqtt_sdk import VdmMqttClient, VdmMqttClientConfig, VdmCodec, VdmTopics, RpcError
from .store import Store
from .modules import alarms, device, measurement, targets
from .modules.evidence import EvidenceService, process_event

LOG = logging.getLogger(__name__)

class ServiceError(Exception):
    def __init__(self, status, code, message, **details):
        self.status = status
        self.body = {"error":{"code":code,"message":message,**details}}
        super().__init__(message)


class Service:
    def __init__(self, settings, *, client_factory=VdmMqttClient, webhook_sender=None):
        self.settings = settings
        self.store = Store(settings)
        self.evidence = EvidenceService(self.store,settings)
        self.devices = {(d['connectionId'],d['deviceId']):d for d in settings.devices}
        self.clients = {}
        self.stop_event = threading.Event()
        self.reconnect = set()
        self.reconnect_lock = threading.Lock()
        self.threads = []
        self.started = False
        self.closed = False
        self.webhook_sender = webhook_sender or self._send_webhook
        self.rpc_slots = threading.BoundedSemaphore(32)
        for (cid,did), spec in self.devices.items():
            digest = hashlib.sha256(json.dumps([cid,did],separators=(',',':')).encode()).hexdigest()[:32]
            config = VdmMqttClientConfig(host=spec['host'],port=spec['port'],topics=VdmTopics.for_device(did),
                payload_format=spec['format'],username=spec['username'],password=spec['password'],
                client_id='vdm-service-python-'+digest,persistent_session=True,manual_ack=True,
                subscription_suffixes=('telemetry','attributes','3A','event','image','rpc/resp'))
            self.clients[(cid,did)] = client_factory(config,
                on_raw_message=lambda topic,raw,c=cid,d=did:self.ingest(c,d,topic,raw),
                on_error=lambda exc,c=cid,d=did:self.transport_error(c,d,exc))

    def get_device(self,cid,did):
        if (cid,did) not in self.devices:
            raise ServiceError(404,'NOT_FOUND','unknown connection/device')
        return self.devices[(cid,did)]

    def ingest(self,cid,did,topic,payload):
        self.get_device(cid,did)
        try:
            self.store.enqueue(cid,did,topic,payload)
        except Exception:
            # Schedule reconnect even if writing a counter also fails (disk full).
            with self.reconnect_lock:
                self.reconnect.add((cid,did))
            try:
                self.store.increment('ingressFailures')
            except Exception:
                pass
            LOG.warning('durable ingress rejected for %s/%s',cid,did)
            raise

    def transport_error(self,cid,did,exc):
        LOG.warning('MQTT %s/%s: %s',cid,did,type(exc).__name__)
        if not self.stop_event.is_set():
            with self.reconnect_lock:
                self.reconnect.add((cid,did))
        try:
            self.store.increment('mqttErrors')
        except Exception:
            pass

    def process_one(self):
        row = self.store.next_pending('inbox')
        if row is None:
            return False
        cid,did = row['connection_id'],row['device_id']
        try:
            spec = self.get_device(cid,did)
            decoded = VdmCodec(spec['format']).decode(row['topic'],VdmTopics.for_device(did),bytes(row['payload']))
            if decoded.suffix == 'image':
                self.evidence.process(cid,did,decoded)
            with self.store.transaction() as db:
                if decoded.suffix == 'telemetry':
                    measurement.process(db,cid,did,decoded.as_dict())
                elif decoded.suffix == 'attributes':
                    device.process(db,cid,did,decoded.as_dict())
                elif decoded.suffix == '3A':
                    alarms.process(db,cid,did,decoded.as_dict(),self.settings.notification_max_age_seconds,self.settings.max_inbox_rows)
                elif decoded.suffix == 'event':
                    process_event(db,cid,did,decoded.as_dict())
                db.execute("UPDATE inbox SET status='done',error=NULL WHERE id=?",(row['id'],))
        except Exception as exc:
            self.store.fail('inbox',row,exc)
            LOG.warning('inbox processing failed id=%s type=%s',row['id'],type(exc).__name__)
        return True

    def invoke(self,cid,did,operation,params):
        spec = self.get_device(cid,did)
        capability = operation['capability']
        if capability != '-' and capability not in spec['capabilities']:
            raise ServiceError(409,'UNSUPPORTED_CAPABILITY',f'operation requires {capability}')
        method = operation['method']
        client = self.clients[(cid,did)]
        if method == 'ackEvidencePackage' and not self.evidence.verified(cid,did,params.get('eventId'),params.get('packageSha256')):
            raise ServiceError(400,'INVALID_ARGUMENT','evidence must be received and verified before ACK')
        try:
            VdmCodec(spec['format']).encode_rpc_request(method,params,1)
        except (TypeError,ValueError) as exc:
            raise ServiceError(400,'INVALID_ARGUMENT',str(exc)) from None
        if not client.is_connected:
            raise ServiceError(503,'UNAVAILABLE','MQTT device connection is unavailable')
        if not self.rpc_slots.acquire(blocking=False):
            raise ServiceError(503,'OVERLOADED','RPC concurrency limit reached')
        try:
            result = (targets.invoke(client,method,params,self.settings.rpc_timeout_ms/1000)
                      if operation['module']=='targets' else
                      client.call(method,params,timeout=self.settings.rpc_timeout_ms/1000))
            response = result.as_dict()
            # Check again for adapters and SDKs configured to return device errors.
            _,code,_,_ = VdmCodec(spec['format']).response_info(getattr(result, 'value', response))
            if code:
                raise ServiceError(502,'DEVICE_ERROR','device rejected request',deviceCode=code)
            return {"connectionId":cid,"deviceId":did,"method":method,
                    "status":"accepted" if operation['mode']=='async' else 'completed',"response":response}
        except ServiceError:
            raise
        except RpcError as exc:
            raise ServiceError(502,'DEVICE_ERROR','device rejected request',deviceCode=exc.code) from None
        except TimeoutError:
            raise ServiceError(504,'RPC_TIMEOUT','device response timed out',outcome='unknown') from None
        except (ValueError,TypeError):
            raise ServiceError(400,'INVALID_ARGUMENT','invalid device request parameters') from None
        except (ConnectionError,OSError,RuntimeError):
            raise ServiceError(503,'UNAVAILABLE','MQTT request failed') from None
        finally:
            self.rpc_slots.release()

    def _call_worker(self,cid,did,method,params):
        client = self.clients[(cid,did)]
        if not client.is_connected:
            raise ConnectionError('device connection unavailable')
        result = client.call(method,params,timeout=self.settings.rpc_timeout_ms/1000)
        response = result.as_dict()
        _,code,_,_ = VdmCodec(self.devices[(cid,did)]['format']).response_info(getattr(result, 'value', response))
        if code:
            raise RpcError(0,code,'worker RPC rejected')
        return response

    def process_job(self):
        row = self.store.next_pending('jobs')
        if row is None:
            return False
        cid,did = row['connection_id'],row['device_id']
        try:
            with self.store.lock:
                watermark = self.store.db.execute('SELECT coalesce(max(rowid),0) FROM alarm_events WHERE connection_id=? AND device_id=?',(cid,did)).fetchone()[0]
            response = self._call_worker(cid,did,'getAlarmState',{})
            with self.store.transaction() as db:
                current = db.execute('SELECT coalesce(max(rowid),0) FROM alarm_events WHERE connection_id=? AND device_id=?',(cid,did)).fetchone()[0]
                if current != watermark:
                    # A live event raced the snapshot request; don't overwrite it.
                    db.execute('UPDATE jobs SET due=? WHERE key=?',(time.time()+1,row['key']))
                    return True
                alarms.reconcile(db,cid,did,response)
                db.execute("UPDATE jobs SET status='done' WHERE key=?",(row['key'],))
        except Exception as exc:
            self.store.fail('jobs',row,exc)
        return True

    def process_ack(self):
        row = self.store.next_pending('evidence')
        if row is None:
            return False
        cid,did = row['connection_id'],row['device_id']
        try:
            if not self.evidence.verified(cid,did,row['event_id'],row['sha']):
                raise ValueError('verified evidence receipt no longer valid')
            kind = 'EVIDENCE_KIND_SNAPSHOT' if self.devices[(cid,did)]['format']=='protobuf' else 'SNAPSHOT'
            self._call_worker(cid,did,'ackEvidencePackage',dict(eventId=row['event_id'],kind=kind,packageSha256=row['sha']))
            with self.store.transaction() as db:
                db.execute("UPDATE evidence SET status='done',error=NULL WHERE key=?",(row['key'],))
        except Exception as exc:
            self.store.fail('evidence',row,exc)
        return True

    def _send_webhook(self,url,payload,headers):
        # No redirects: do not forward bearer secrets to another host.
        with httpx.Client(timeout=5.0,follow_redirects=False,trust_env=False) as client:
            response = client.post(url,json=payload,headers=headers)
            if not 200 <= response.status_code < 300:
                raise RuntimeError('webhook rejected delivery')

    def process_outbox(self):
        if not self.settings.webhook_url:
            return False
        row = self.store.next_pending('outbox')
        if row is None:
            return False
        headers = {'Idempotency-Key':row['key']}
        if self.settings.webhook_token:
            headers['Authorization']='Bearer '+self.settings.webhook_token
        try:
            self.webhook_sender(self.settings.webhook_url,json.loads(row['payload']),headers)
            with self.store.transaction() as db:
                db.execute("UPDATE outbox SET status='done',error=NULL WHERE key=?",(row['key'],))
        except Exception as exc:
            self.store.fail('outbox',row,exc)
        return True

    def health(self):
        connections = [dict(connectionId=c,deviceId=d,connected=bool(client.is_connected)) for (c,d),client in self.clients.items()]
        counters = self.store.counters()
        degraded = any(not item['connected'] for item in connections) or any(v for k,v in counters.items() if k.endswith('_failed'))
        return dict(status='degraded' if degraded else 'ok',connections=connections,counters=counters)

    def _loop(self,work):
        while not self.stop_event.is_set():
            try:
                if work():
                    continue
            except Exception as exc:
                LOG.error('worker failure: %s',type(exc).__name__)
            self.stop_event.wait(.1)

    def prune(self):
        self.evidence.prune()
        self.store.prune()

    def _maintenance(self):
        while not self.stop_event.wait(1):
            with self.reconnect_lock:
                reconnect,self.reconnect = self.reconnect,set()
            for key in reconnect:
                try:
                    self.clients[key].reconnect_background()
                except Exception as exc:
                    self.transport_error(*key,exc)
                    with self.reconnect_lock:
                        self.reconnect.add(key)
            try:
                self.prune()
            except Exception as exc:
                LOG.error('retention failed: %s',type(exc).__name__)

    def start(self):
        if self.started:
            return
        self.started = True
        for key,client in self.clients.items():
            try:
                client.start_background()
            except Exception as exc:
                self.transport_error(*key,exc)
                with self.reconnect_lock:
                    self.reconnect.add(key)
        for work in (self.process_one,self.process_job,self.process_ack,self.process_outbox):
            thread = threading.Thread(target=self._loop,args=(work,),daemon=True)
            thread.start(); self.threads.append(thread)
        thread = threading.Thread(target=self._maintenance,daemon=True)
        thread.start(); self.threads.append(thread)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.stop_event.set()
        for client in self.clients.values():
            client.stop()
        for thread in self.threads:
            thread.join(timeout=self.settings.rpc_timeout_ms/1000+6)
        self.store.close()
