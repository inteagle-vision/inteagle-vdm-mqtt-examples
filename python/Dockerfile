FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY python/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proto/inteagle_vdm_mqtt_v1.proto /app/proto/inteagle_vdm_mqtt_v1.proto
RUN python -m grpc_tools.protoc \
        -I/app/proto \
        --python_out=/app \
        /app/proto/inteagle_vdm_mqtt_v1.proto

COPY python/common.py python/consumer.py python/publisher.py python/alarm_rpc.py python/receive_data.py python/evidence_receiver.py /app/
COPY python/vdm_mqtt_sdk /app/vdm_mqtt_sdk
COPY examples /examples
COPY python/tests /app/tests
RUN python -m unittest discover -s /app/tests -v

CMD ["python", "consumer.py"]
