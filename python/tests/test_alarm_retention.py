"""Keep active/ambiguous lifecycles, retire closed ones, bound reconciliation work."""
import json
import time
import unittest
import test_service as base
from vdm_service.store import identity


class AlarmRetentionTests(base.ServiceFixture):
    def test_only_old_unambiguous_closed_lifecycles_expire(self):
        now=int(time.time())
        for aid,transition,ts in [('1','RECOVERED',1),('2','TRIGGERED',1),
                                   ('3','CANCELLED',1),('4','RECOVERED',1),
                                   ('5','TRIGGERED',1),('6','TRIGGERED',1),
                                   ('7','RECOVERED',now),('8','TRIGGERED',1)]:
            self.ingest('3A',{**self.alarm(aid,ts,transition),'alarmId':aid})
        with self.svc.store.transaction() as db:
            for aid,fields,ambiguous in [
                ('3',{},True),('4',{'currentActive':True},False),
                ('5',{'currentActive':False,'reconciledAt':1},False),
                ('6',{'currentActive':False,'reconciledAt':now},False),
                ('8',{'currentActive':False,'reconciledAt':1},True)]:
                payload=json.loads(db.execute('SELECT payload FROM alarms WHERE alarm_id=?',(aid,)).fetchone()[0])
                payload.update(fields)
                db.execute('UPDATE alarms SET payload=?,needs_reconcile=? WHERE alarm_id=?',(json.dumps(payload),int(ambiguous),aid))
        self.svc.prune()
        remaining={item['alarmId'] for item in self.svc.store.alarms('local','DEMO')}
        self.assertEqual(remaining,{'2','3','4','6','7','8'})

    def test_pending_and_failed_reconciliation_jobs_share_capacity(self):
        self.svc.settings.max_inbox_rows=1
        event=self.alarm('1',transition='SYNCED')
        self.ingest('3A',event)
        with self.svc.store.transaction() as db:
            db.execute("INSERT INTO jobs(key,connection_id,device_id,status,created) VALUES('occupied','other','DEMO','failed',0)")
        self.ingest('3A',self.alarm('2',event['ts'],'RECOVERED'))
        self.assertEqual(self.svc.store.count('jobs'),1)
        self.assertEqual(self.svc.store.count('alarm_events'),1)
        self.assertEqual(self.svc.store.counters()['inbox_pending'],1)
        # Refreshing the same per-device job consumes no additional queue slot.
        with self.svc.store.transaction() as db:
            db.execute('UPDATE jobs SET key=?,connection_id=?',(identity('local','DEMO','reconcile'),'local'))
            db.execute('UPDATE inbox SET due=0')
        self.svc.process_one()
        self.assertEqual(self.svc.store.count('jobs'),1)
        self.assertEqual(self.svc.store.counters()['jobs_pending'],1)
        self.assertTrue(self.svc.store.alarms('local','DEMO')[0]['needsReconcile'])

if __name__=='__main__':unittest.main()
