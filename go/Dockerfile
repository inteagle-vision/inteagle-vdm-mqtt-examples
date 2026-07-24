# syntax=docker/dockerfile:1.7

FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/* \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5

WORKDIR /src
COPY go/go.mod go/go.sum /src/go/
WORKDIR /src/go
RUN go mod download

COPY proto/inteagle_vdm_mqtt_v1.proto /src/proto/inteagle_vdm_mqtt_v1.proto
RUN mkdir -p /src/go/generated \
    && protoc \
        -I/src/proto \
        --go_out=/src/go/generated \
        --go_opt=paths=source_relative \
        /src/proto/inteagle_vdm_mqtt_v1.proto

COPY go/cmd /src/go/cmd
COPY go/sdk /src/go/sdk
RUN go test ./... \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vdm-consumer ./cmd/consumer

FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35
COPY --from=builder /out/vdm-consumer /vdm-consumer
ENTRYPOINT ["/vdm-consumer"]
