"""FastAPI adapter for the shared 33-operation business contract."""
from contextlib import asynccontextmanager
import hmac
import json
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from .config import Settings
from .runtime import Service, ServiceError
from .modules.device import public_metadata

OPERATIONS = json.loads((Path(__file__).resolve().parents[2]/'contracts/operations.json').read_text())


def create_app(service=None, *, manage_lifecycle=True):
    service = service or Service(Settings.load())
    @asynccontextmanager
    async def lifespan(app):
        if manage_lifecycle:
            service.start()
        try:
            yield
        finally:
            if manage_lifecycle:
                await run_in_threadpool(service.close)
    app = FastAPI(title='VDM Python reference service',version='1.0.0',lifespan=lifespan,
                  docs_url=None,redoc_url=None,openapi_url=None)
    app.state.service = service

    @app.middleware('http')
    async def authenticate(request,call_next):
        token = service.settings.api_token
        if request.url.path != '/health' and token and not hmac.compare_digest(request.headers.get('authorization','').encode(),('Bearer '+token).encode()):
            return JSONResponse({'error':{'code':'UNAUTHORIZED','message':'valid bearer token required'}},status_code=401)
        return await call_next(request)

    @app.exception_handler(ServiceError)
    async def service_error(request,error):
        return JSONResponse(error.body,status_code=error.status)

    @app.exception_handler(HTTPException)
    async def http_error(request,error):
        return JSONResponse({'error':{'code':'NOT_FOUND' if error.status_code==404 else 'INVALID_ARGUMENT','message':str(error.detail)}},status_code=error.status_code)

    @app.get('/health')
    def health():
        return service.health()

    @app.get('/v1/devices')
    def devices():
        return {'devices':[public_metadata(item) for item in service.devices.values()]}

    prefix = '/v1/connections/{connectionId}/devices/{deviceId}/'
    @app.get(prefix+'latest')
    def latest(connectionId:str,deviceId:str):
        service.get_device(connectionId,deviceId)
        return service.store.latest(connectionId,deviceId)

    @app.get(prefix+'alarms/local')
    def alarms(connectionId:str,deviceId:str):
        service.get_device(connectionId,deviceId)
        return {'items':service.store.alarms(connectionId,deviceId)}

    def endpoint(operation):
        async def invoke(connectionId:str,deviceId:str,request:Request):
            service.get_device(connectionId,deviceId)
            raw = bytearray()
            async for part in request.stream():
                raw.extend(part)
                if len(raw)>1024*1024:
                    raise ServiceError(400,'INVALID_ARGUMENT','request body exceeds 1 MiB')
            try:
                def invalid_constant(value):
                    raise ValueError('non-finite number')
                body = json.loads(raw,parse_constant=invalid_constant) if raw else {}
            except (ValueError,UnicodeDecodeError,RecursionError):
                raise ServiceError(400,'INVALID_ARGUMENT','body must be valid JSON') from None
            if not isinstance(body,dict) or set(body)-{'params'} or not isinstance(body.get('params',{}),dict):
                raise ServiceError(400,'INVALID_ARGUMENT','body must contain only an object params')
            result = await run_in_threadpool(service.invoke,connectionId,deviceId,operation,body.get('params',{}))
            return JSONResponse(result,status_code=202 if operation['mode']=='async' else 200)
        return invoke

    for operation in OPERATIONS:
        app.add_api_route(prefix+operation['route'],endpoint(operation),methods=['POST'],name=operation['method'],tags=[operation['module']])
    return app
