#!/usr/bin/env python3
"""Generate the common OpenAPI paths from the reviewed public operation list."""
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
ops=json.loads((ROOT/'contracts/operations.json').read_text())
ref=lambda n:{'$ref':'#/components/schemas/'+n}
responses={str(code):{'description':desc,'content':{'application/json':{'schema':ref('Error')}}} for code,desc in [(400,'Invalid input'),(401,'Authentication required'),(404,'Unknown tuple/route'),(409,'Unsupported configured capability'),(502,'Device rejected request'),(503,'Unavailable or overloaded'),(504,'Unknown RPC outcome; read back before retry')]}
params=[{'name':x,'in':'path','required':True,'schema':{'type':'string','pattern':'^[A-Za-z0-9_-]{1,128}$'}} for x in ['connectionId','deviceId']]
prefix='/v1/connections/{connectionId}/devices/{deviceId}'
paths={}
for route,name in [('/health','health'),('/v1/devices','devices'),(prefix+'/latest','latest'),(prefix+'/alarms/local','localAlarms')]:
 paths[route]={'get':{'operationId':name,'parameters':params if '{' in route else [],'responses':{'200':{'description':'Local service state','content':{'application/json':{'schema':{'type':'object'}}}}},**({'security':[]} if route=='/health' else {})}}
for op in ops:
 paths[prefix+'/'+op['route']]={'post':{'operationId':op['method'],'tags':[op['module']],'summary':op['method'],
 'description':f"Device RPC {op['method']}. Required configured capability: {op['capability']}. Params follow device JSON or ProtoJSON according to configured format. Device errors are never reported as success. No automatic mutation retry.",
 'parameters':params,'requestBody':{'required':False,'content':{'application/json':{'schema':ref('Request')}}},
 'responses':{**responses,str(202 if op['mode']=='async' else 200):{'description':'Accepted by device; confirm using events/query' if op['mode']=='async' else 'Device RPC response received','content':{'application/json':{'schema':ref('Result')}}}}}}
spec={'openapi':'3.0.3','info':{'title':'VDM reference services','version':'1.0.0'},'servers':[{'url':'http://127.0.0.1:8080'}],'security':[{'bearerAuth':[]}], 'paths':paths,'components':{'securitySchemes':{'bearerAuth':{'type':'http','scheme':'bearer','description':'Required when VDM_API_TOKEN is set; mandatory for remote bind'}},'schemas':{
 'Request':{'type':'object','additionalProperties':False,'properties':{'params':{'type':'object','additionalProperties':True,'default':{}}}},
 'Result':{'type':'object','required':['connectionId','deviceId','method','status','response'],'properties':{'connectionId':{'type':'string'},'deviceId':{'type':'string'},'method':{'type':'string','enum':[o['method'] for o in ops]},'status':{'type':'string','enum':['completed','accepted']},'response':{'type':'object','description':'Unmodified decoded device response. Protobuf uint64 values are decimal strings.'}}},
 'Error':{'type':'object','required':['error'],'properties':{'error':{'type':'object','required':['code','message'],'properties':{'code':{'type':'string'},'message':{'type':'string'},'outcome':{'type':'string','enum':['unknown']},'deviceCode':{'type':'integer'}}}}}}}
}
# Detailed shared local resource schemas; device wire payload stays format-specific.
schemas=spec['components']['schemas']
schemas['Device']={'type':'object','required':['connectionId','deviceId','format','capabilities'],'properties':{'connectionId':{'type':'string'},'deviceId':{'type':'string'},'format':{'type':'string','enum':['json','protobuf']},'capabilities':{'type':'array','items':{'type':'string'}}}}
schemas['Latest']={'type':'object','required':['telemetry','attributes','receivedCount'],'properties':{'telemetry':{'type':'object','nullable':True},'attributes':{'type':'object','nullable':True},'receivedCount':{'type':'integer','minimum':0}}}
schemas['LocalAlarm']={'type':'object','required':['alarmId','eventId','ts','transition','needsReconcile'],'properties':{'alarmId':{'type':'string'},'eventId':{'type':'string'},'ts':{'description':'Device UTC seconds, number for JSON or decimal string for ProtoJSON','oneOf':[{'type':'integer'},{'type':'string'}]},'transition':{'type':'string'},'needsReconcile':{'type':'boolean'},'currentActive':{'type':'boolean'},'currentState':{'type':'object','nullable':True},'reconciledAt':{'type':'number','description':'Platform UTC seconds for state query; never substitutes device event time'}}}
paths[prefix+'/latest']['get']['responses']['200']['content']['application/json']['schema']=ref('Latest')
paths[prefix+'/alarms/local']['get']['responses']['200']['content']['application/json']['schema']={'type':'object','required':['items'],'properties':{'items':{'type':'array','items':ref('LocalAlarm')}}}
paths['/v1/devices']['get']['responses']['200']['content']['application/json']['schema']={'type':'object','required':['devices'],'properties':{'devices':{'type':'array','items':ref('Device')}}}
for case in json.loads((ROOT/'tests/fixtures/rpc-cases.json').read_text()):
    paths[prefix+'/'+case['route']]['post']['requestBody']['content']['application/json']['examples']={fmt:{'value':{'params':case[fmt]}} for fmt in ['json','protobuf']}
(ROOT/'contracts/openapi.json').write_text(json.dumps(spec,ensure_ascii=False,indent=2)+'\n')
