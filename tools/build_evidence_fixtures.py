#!/usr/bin/env python3
"""Deterministic protocol-only USTAR evidence fixture, no real device data."""
import hashlib,io,json,struct,tarfile
from pathlib import Path
root=Path(__file__).resolve().parents[1]/'tests/fixtures'
stream=io.BytesIO()
with tarfile.open(fileobj=stream,mode='w',format=tarfile.USTAR_FORMAT) as archive:
    for name,data in [('manifest.json',b'{"eventId":"9001"}'),('frame-000.jpg',b'\xff\xd8'+bytes(140000)+b'\xff\xd9')]:
        info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o600;info.mtime=0
        archive.addfile(info,io.BytesIO(data))
package=stream.getvalue();sha=hashlib.sha256(package).digest();count=(len(package)+131071)//131072
for index in range(count):
    offset=index*131072;data=package[offset:offset+131072]
    header=bytes([2,76,1,1])+struct.pack('>QQ',9001,len(package))+sha+struct.pack('>IIQII',index,count,offset,len(data),0)
    (root/f'evidence-{index}.bin').write_bytes(header+data)
(root/'evidence.json').write_text(json.dumps({'eventId':'9001','packageSha256':sha.hex(),'packageLength':len(package),'chunkCount':count},indent=2)+'\n')
