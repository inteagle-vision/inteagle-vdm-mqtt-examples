#!/usr/bin/env python3
"""Print or send one concrete SDK business request; defaults to print-only."""
import argparse
import json
import os
from pathlib import Path
from vdm_mqtt_sdk import VdmCodec, VdmMqttClient, VdmMqttClientConfig, VdmTopics


def requests_for(profile):
    proto = profile == 'protobuf'
    target = {'targetId':'T01','sensorId':0,'roi':{'x':800,'y':400,'width':400,'height':400},
              'distanceM' if proto else 'distance':6.0,
              'role':'TARGET_ROLE_MP' if proto else 'MP',
              'targetModel':'TARGET_MODEL_T100' if proto else 'T100','skipMeasurement':False}
    sync = ({'telemetryType':'TELEMETRY_SYNC_TYPE_DISPLACEMENT','startTs':'1788170700','endTs':'1788181500','targetIds':['T01']}
            if proto else {'type':'displacement','startTs':1788170700,'endTs':1788181500,'targetIds':['T01']})
    rules = json.loads((Path(__file__).resolve().parents[1]/'examples/alarms/requests.json').read_text())
    rule = next(case[profile] for case in rules if case['name']=='create-displacement')
    return {
        'device-attributes':('getAttr',{'keys':['deviceId']}),
        'device-time':('syncTime',{'ntpServer':'ntp.aliyun.com'}),
        'device-lights':('setLightLevel',{'allLightsLevel':3} if proto else {'level':3}),
        'targets-query':('getTargets',{}),
        'targets-add':('addTargets',{'targets':[target]}),
        'targets-update':('setTargets',{'targets':[{'targetId':'T01','distanceM' if proto else 'distance':8.0}]}),
        'targets-delete':('deleteTargets',{'targetIds':['T01']}),
        'targets-initialize':('initRefTargets',{'targets':[{'targetId':'T01','sensorId':0}]}),
        'measurement-start':('startMeasurement',{}),
        'measurement-stop':('stopMeasurement',{}),
        'measurement-sync':('syncTelemetry',sync),
        'measurement-status':('getSyncStatus',{'jobId':'REPLACE_WITH_JOB_ID'}),
        'measurement-cancel':('cancelSync',{'jobId':'REPLACE_WITH_JOB_ID'}),
        'alarms-caps':('getAlarmCaps',{}),
        'alarms-rules':('applyAlarmRules',rule),
        'alarms-state':('getAlarmState',{}),
        'alarms-events':('listAlarmEvents',{'page':1,'pageSize':20,'alarmId':'9007199254740900'}),
        'evidence-snapshot':('snapshot',{'sensorId':0}),
        'evidence-status':('getEvidenceStatus',{'eventId':'9007199254740993','kind':'EVIDENCE_KIND_SNAPSHOT' if proto else 'SNAPSHOT'}),
        'evidence-retry':('retryEvidence',{'eventId':'9007199254740993','kind':'EVIDENCE_KIND_SNAPSHOT' if proto else 'SNAPSHOT'}),
    }


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case',nargs='?',default='device-attributes')
    parser.add_argument('--format',choices=['json','protobuf'],default=os.getenv('VDM_PAYLOAD_FORMAT','protobuf'))
    parser.add_argument('--send',action='store_true',help='Actually send the selected request to the device')
    parser.add_argument('--params',help='JSON object overriding demonstration parameters')
    args=parser.parse_args()
    cases=requests_for(args.format)
    if args.case not in cases:
        parser.error('case must be one of: '+', '.join(cases))
    method,params=cases[args.case]
    if args.params:
        params=json.loads(args.params)
        if not isinstance(params,dict): parser.error('--params must be an object')
    payload,_=VdmCodec(args.format).encode_rpc_request(method,params,1)
    print(json.dumps({'method':method,'params':params,'format':args.format,'wireBytes':len(payload)},ensure_ascii=False,indent=2))
    if not args.send:
        return
    with VdmMqttClient(VdmMqttClientConfig(host=os.environ['MQTT_HOST'],port=int(os.getenv('MQTT_PORT','1883')),
        topics=VdmTopics.for_device(os.environ['VDM_DEVICE_ID']),payload_format=args.format,
        username=os.getenv('MQTT_USERNAME'),password=os.getenv('MQTT_PASSWORD'))) as client:
        print(json.dumps(client.call(method,params).as_dict(),ensure_ascii=False,indent=2))

if __name__=='__main__':main()
