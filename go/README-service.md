# Go VDM reference service

Go 1.23+、标准 `net/http`、Paho MQTT 和 SQLite WAL 的单进程参考服务。旧的 `consumer`、`receive-data`、`alarm-rpc`、`evidence-receiver` CLI 保留；需要持久化、自动证据确认和 HTTP 接口时运行新的 `service`。

## 构建与启动

在仓库 `go/` 目录执行；Protobuf 生成文件不提交。

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p generated
protoc -I../proto --go_out=generated --go_opt=paths=source_relative ../proto/inteagle_vdm_mqtt_v1.proto
go mod download
go test ./...
go run ./cmd/service
```

需要 `protoc` 3.15+。依赖版本锁在 `go.mod` / `go.sum`：Paho `v1.5.0`、Protobuf `v1.36.5`、modernc SQLite `v1.34.5`（其发布的 go.mod 声明 Go 1.21，兼容本项目 Go 1.23）。版本和手动 ACK API 已通过官方模块索引及下载的模块源码核对：

- [SQLite v1.34.5 模块声明](https://proxy.golang.org/modernc.org/sqlite/@v/v1.34.5.mod)
- [Paho v1.5.0 API](https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang@v1.5.0#ClientOptions.SetAutoAckDisabled)
- [Protobuf v1.36.5 模块声明](https://proxy.golang.org/google.golang.org/protobuf/@v/v1.36.5.mod)

配置格式和完整 33 个公开路由以 `../contracts/service.md`、`../contracts/operations.json` 为准。示例配置仅含本机地址和占位设备 ID。

| 环境变量 | 默认 / 用途 |
|---|---|
| `VDM_SERVICE_CONFIG` | `../contracts/config.example.json` |
| `VDM_DATA_DIR` | `./data`，数据库及证据持久目录 |
| `VDM_HTTP_HOST` | `127.0.0.1` |
| `VDM_HTTP_PORT` | `8080` |
| `VDM_API_TOKEN` | 设置后除 `/health` 外全部要求 Bearer 认证；非回环绑定必须设置 |
| `VDM_WEBHOOK_URL` | 可选下游告警接收 URL；未设置时 outbox 保留待发 |
| `VDM_WEBHOOK_TOKEN` | 可选下游 Bearer 密钥 |

MQTT 用户名和密码只从配置 `usernameEnv` / `passwordEnv` 所指定的环境变量读取。不要把凭据写入配置。连接和设备 ID 只能包含字母、数字、下划线和连字符。每个连接内设备 ID 唯一；相同设备 ID 可以存在于不同连接。`capabilities` 必须是已核实的硬件能力，当前矩阵要求电机和巡航查询使用 `motor`；ISP 不附加能力门禁。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/devices
curl -H 'Content-Type: application/json' \
  -d '{"params":{"keys":["sampleFrequencyHz"]}}' \
  http://127.0.0.1:8080/v1/connections/local/devices/DEMO/device/attributes/query
```

设置 API token 后添加 `Authorization: Bearer ...`。HTTP 请求体最大 1 MiB，必须为 `{ "params": { ... } }` 、`{}` 或省略请求体。JSON 设备使用设备 JSON 参数，Protobuf 设备使用 ProtoJSON 参数；64 位标识保持十进制字符串。异步操作仅在收到成功 RPC 响应后返回 202，完成状态仍须查询或订阅；超时返回 504、`outcome: unknown`，服务不自动重试用户的修改命令。

## Docker

从仓库根目录构建，启动入口为 `/vdm-service`：

```sh
docker build -f go/Dockerfile.service -t vdm-go-service .
# VDM_API_TOKEN 应先由调用环境安全设置。
docker run --rm -p 127.0.0.1:8080:8080 \
  -e VDM_HTTP_HOST=0.0.0.0 -e VDM_API_TOKEN \
  -e MQTT_USERNAME -e MQTT_PASSWORD \
  -v "$PWD/contracts/config.example.json:/config.json:ro" \
  -e VDM_SERVICE_CONFIG=/config.json \
  -v vdm-go-data:/data vdm-go-service
```

容器配置中的 `127.0.0.1` 指容器自身；连接 Compose Broker 时使用对应服务名。旧 `go/Dockerfile` 继续提供旧 CLI 镜像。数据目录需由容器 nonroot 用户可写。

## 业务 SDK 示例

每个业务域都有实际参数示例，默认只打印并校验编码。先替换 `internal/businessexamples/requests.go` 中的示例设备字段、标靶、历史时间、jobId、alarmId、eventId，再对测试设备执行。`targets` 会修改标靶，`measurement` 会启动历史补传；示例时间固定，不代表设备上必然有该段数据。

```sh
go run ./cmd/business-example --case device
go run ./cmd/business-example --case targets
go run ./cmd/business-example --case measurement
go run ./cmd/business-example --case measurement-status
go run ./cmd/business-example --case measurement-cancel
go run ./cmd/business-example --case alarms
go run ./cmd/business-example --case evidence
# 切换 JSON；Protobuf 是这些 CLI 的默认格式。
VDM_PAYLOAD_FORMAT=json go run ./cmd/business-example --case measurement
# MQTT_HOST / VDM_DEVICE_ID / 凭据由外部环境设置。
go run ./cmd/business-example --case device --execute
```

补传 JSON 使用 `type: "displacement"`，ProtoJSON 使用 `telemetryType: "TELEMETRY_SYNC_TYPE_DISPLACEMENT"`，与固件公开接口一致。原有 `alarm-rpc` 支持完整规则配置请求和 `--print-only`。

## 数据流和可靠性

- 每个 `(connectionId, deviceId)` 单独 SDK 客户端、独立 RPC pending map、稳定 Go 专属 client ID、MQTT 持久会话。Broker 不可用时 HTTP 保持运行，`/health` 显示 degraded，后台继续连接。
- MQTT 业务回调只将原始消息同步写入有界 SQLite inbox；事务提交后才 PUBACK。失败或积压容量用尽时不确认 QoS1，断开后重连等待 Broker 重投。RPC 响应直接解析并关联；回调不执行任何 RPC。QoS0 只能尽力接收。
- WAL、`synchronous=FULL`、`user_version=1` 迁移和目录排他锁。进程中只有一个 inbox worker、一个 webhook worker、一个后台 RPC worker；HTTP RPC 并发上限 32。
- 最新 telemetry/attributes 与处理状态同一事务提交。告警事件按源设备和 eventId 去重；状态按 alarmId 组织；事件、状态、outbox 同一事务。只有严格更大的设备秒时间更新状态。等时间和旧事件保留、标记 `needsReconcile`，持久调度 `getAlarmState`；查询结果作为 `currentActive`、`currentState`、`reconciledAt` 保存，不捏造新的告警转换。
- SYNCED 只更新状态，DEESCALATED 不通知。TRIGGERED 仅首次生命周期触发通知；ESCALATED 仅首次观测或等级严格提升时通知；新 RECOVERED、CANCELLED 通知，且抑制过期和未来时间。稳定 SHA-256 幂等键写入 webhook `Idempotency-Key`；5 秒超时、最多 8 次指数退避。下游必须去重；不承诺 exactly-once。
- `/event` 证据状态按完整规范化 payload 去重，与告警通知分离。图像分块先保存到 inbox，然后保留到 evidence_chunks；重启时从持久分块恢复 SDK 组包。SDK 校验范围、SHA-256、USTAR 普通文件和安全路径。完整包及 receipt 持久化后，数据库事务创建 ACK 任务；失败和重启后继续重试。REST `evidence/ack` 也要求匹配同设备的已校验 receipt，防止提前确认。
- 普通 JPEG 按内容哈希持久保存。`maxEvidenceBytes` 限制证据文件与待组包分块的逻辑字节，容量不足会显示处理失败。每小时按 retentionDays 清理已处理 inbox、历史事件、完成的 outbox/jobs，以及过期普通 JPEG 和已成功 ACK 的证据包；删除包时同时作废本地 receipt。待处理、失败 ACK 和未完整组包的 .part 文件保留；已处理 alarm/evidence 事件历史各自按最早记录清理到最多 10×maxInboxRows；已结束且无待核对标记的旧生命周期按保留期清理，权威快照仍活动或最近才确认关闭的记录保留。

## 运维边界与验证

`/health.counters` 包含 inbox/outbox/jobs 的 pending/failed 数及入队失败次数。格式错误、配额不足和重试耗尽不会静默丢弃；记录保留在数据库，需运维人员排除原因后重新置为 pending（同时清零 attempts/next_at），或明确归档删除。未配置 webhook 时待发记录持续保留；pending+failed outbox 及后台 jobs 各自上限为 maxInboxRows。达到上限时，告警事件、状态和通知事务整体回滚，原始 inbox 留待重试，不淘汰未完成通知。应配置下游或处理失败记录以恢复处理。证据保留期按 retentionDays 执行；普通 JPEG 和已成功 ACK 的包会自动删除。需要长期保存时应在保留期内归档；不要删除待处理 chunks/part 文件。

本实现面向 Linux 单进程，使用 `flock`，不能多个副本共用数据目录。MQTT TCP 未实现 TLS 参数，跨不可信网络需要安全代理或在 SDK 层扩展 TLS 配置；HTTP TLS 可由反向代理终止。没有设备在线心跳判定：health 中 connected 代表 Broker 连接和订阅成功。业务模块是清晰的扩展边界，设备字段级约束仍由固件校验；配置 capability 不代表硬件实测。

为简化可靠恢复，收到新分块时会重放该包已存分块，组包 IO 最坏随分块数平方增长；单包 SDK 上限 32 MiB；普通图像消息同样最多 32 MiB，并受 maxEvidenceBytes 约束。证据上限是逻辑 payload/file 大小，不含 SQLite 页、WAL、索引开销；应另设磁盘配额和磁盘空间告警。重试预算耗尽的证据 ACK 会留在 failed jobs 中，需人工处理；设备可通过 retryEvidence 重投，但已完成的相同 ACK 任务不会重复执行。

测试覆盖 HTTP 参数/认证/能力/错误映射、33 路由契约和双格式编码、持久化重启、目录排他、告警去重/顺序/通知、webhook 重试幂等、证据重启组包/哈希失败/ACK 恢复。`go test ./...`、`go test -race ./...`、`go vet ./...` 可独立运行。Broker 冒烟测试只能证明协议联通和模拟响应，不替代真机的电机、ISP、巡航、图像质量或测量验收。
