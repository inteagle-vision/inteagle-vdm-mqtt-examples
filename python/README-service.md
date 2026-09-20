# Python VDM 参考服务

Python 3.11+。服务入口为 `python -m vdm_service`；SDK、原有 CLI 入口继续保留。
HTTP 业务契约来自 [service.md](../contracts/service.md)，33 个固定路由来自
[operations.json](../contracts/operations.json)。不提供任意方法名 RPC 代理。

## 本地启动

在仓库根目录执行：

```bash
python3 -m venv python/.venv
python/.venv/bin/pip install -r python/requirements.lock
python/.venv/bin/python -m grpc_tools.protoc -I proto --python_out=python proto/inteagle_vdm_mqtt_v1.proto
cp contracts/config.example.json /tmp/vdm-python-config.json
cd python
export VDM_SERVICE_CONFIG=/tmp/vdm-python-config.json
export VDM_DATA_DIR=./data
.venv/bin/python -m vdm_service
```

配置中每个 `connections[].devices[]` 对应一个独立 SDK 客户端、请求关联表和稳定的
MQTT client ID。不同连接下同名设备的状态相互隔离。修改配置后重启服务。用户名与密码
仅通过配置里的 `usernameEnv`、`passwordEnv` 引用环境变量；不要写入 JSON 文件。
只配置已验证的硬件能力：需要 `motor` 的路由会在调用设备前检查 capability；
实际设备仍可能返回不支持错误，以设备数字错误码为准。

| 环境变量 | 默认值 / 用途 |
|---|---|
| `VDM_SERVICE_CONFIG` | 仓库 `contracts/config.example.json` |
| `VDM_DATA_DIR` | 当前目录 `./data`；一个目录只能有一个服务进程 |
| `VDM_HTTP_HOST` / `VDM_HTTP_PORT` | `127.0.0.1` / `8080` |
| `VDM_API_TOKEN` | 设置后，除 `/health` 外所有 HTTP 路由需要 Bearer token |
| `VDM_WEBHOOK_URL` | 可选；未配置时通知留在本地 outbox |
| `VDM_WEBHOOK_TOKEN` | 可选；仅通过环境传入的 webhook Bearer token |

非回环地址绑定必须设置 `VDM_API_TOKEN`。容器对外提供 HTTP 时设置
`VDM_HTTP_HOST=0.0.0.0` 并从部署环境注入 token。认证不会自动提供 TLS；需要加密传输时
由部署方提供 HTTPS 反向代理和受保护的 MQTT 网络。

```bash
docker build -f python/Dockerfile.service -t vdm-python-service .
```

镜像以 UID 10001 运行，数据目录 `/data`，入口与本地相同。挂载的目录需允许该 UID 写入。
`python/Dockerfile` 仍默认运行原有 `consumer.py`。两个镜像都会重新生成 Protobuf 并运行测试。

## HTTP 与 SDK 示例

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/devices
curl -X POST http://127.0.0.1:8080/v1/connections/local/devices/DEMO/device/attributes/query \
  -H 'Content-Type: application/json' -d '{"params":{"keys":["deviceId"]}}'
curl -X POST http://127.0.0.1:8080/v1/connections/local/devices/DEMO/measurement/sync \
  -H 'Content-Type: application/json' \
  -d '{"params":{"type":"displacement","startTs":1788170700,"endTs":1788181500,"targetIds":["T01"]}}'
```

上述补传请求适用于 JSON 连接；Protobuf 连接使用 ProtoJSON：
`{"params":{"telemetryType":"TELEMETRY_SYNC_TYPE_DISPLACEMENT","startTs":"1788170700","endTs":"1788181500","targetIds":["T01"]}}`。
时间戳单位为秒，64 位 ID 使用十进制字符串。JSON 的 `distance` 与 ProtoJSON 的 `distanceM`
等字段并不相同；服务保留设备协议原义，不猜测或转换字段。设备的字段、取值和业务状态校验
失败以 `502 DEVICE_ERROR` 和 `deviceCode` 返回。

```bash
# SDK 最小业务示例：默认只打印并验证编码，不连接设备
python sdk_examples.py targets-add --format protobuf
python sdk_examples.py measurement-sync --format json
python sdk_examples.py alarms-rules --format protobuf
python sdk_examples.py evidence-status --format protobuf
# --send 才实际发送；设备和凭据通过环境配置
export MQTT_HOST=127.0.0.1 MQTT_PORT=1883 VDM_DEVICE_ID=DEMO
python sdk_examples.py device-attributes --format json --send
```

`sdk_examples.py` 覆盖设备属性/时间/补光、标靶增删改与初始化、测量及补传、规则与事件、
抓拍与证据重试。`--params '{...}'` 可覆盖示例参数；目标 ROI、标靶 ID、时间范围和任务 ID
需要改为现场值。规则例子引用已有共享告警 fixture，初始规则为 disabled。
返回 `202 accepted` 只表示异步 RPC 被设备成功接受；不表示抓拍、重启或补传已经完成。
超时返回 `504 RPC_TIMEOUT`、`outcome: unknown`，服务不会自动重试修改类 HTTP RPC。

## 持久化与恢复

- SQLite 使用 WAL、`synchronous=FULL`、`user_version=1`；进程持有数据目录文件锁。
- MQTT 业务消息先同步提交原始 inbox，再发 QoS 1 PUBACK。RPC 响应直接唤醒对应调用者。
  MQTT 回调不执行证据 ACK RPC、通知投递或业务状态处理。QoS 0 只能尽力接收。
- inbox 受 `maxInboxRows` 限制，普通 MQTT payload 上限 1 MiB；image 可达 32 MiB（加二进制头），
  同时受 `maxEvidenceBytes` 限制。满时只回收已处理行，
  不移除 pending/failed；仍无空间时不 PUBACK，并重新连接持久会话请求重投。
  未完成通知及 pending/failed 补查任务分别受 `maxInboxRows` 限制，满时回滚事件事务，保留 inbox 待处理；
  同一设备重用补查任务不重复占用容量。
- 设备 telemetry / attributes 分别保存最新一条及总接收次数；不是历史时序数据库。
- 告警按 `(connectionId, deviceId, eventId)` 去重。事件、状态和 outbox 同一事务提交。
  仅严格更新的 `ts` 推进事件状态；相同/更旧时间保留历史并设置 `needsReconcile`，
  持久安排 `getAlarmState`。快照更新 `status`、`currentActive`、`currentState`、`reconciledAt`，保留原始
  `eventId`、`transition` 和 `ts`；不会用收到消息的本地时间替换设备事件时间。
- 同一 alarmId 仅首次 TRIGGERED 通知；ESCALATED 仅在没有先前状态或等级严格升高时通知。
  新鲜且未重复的 RECOVERED/CANCELLED 通知；SYNCED、DEESCALATED、过期和未来事件不触发通知。`/event` 的证据状态独立保存，
  用完整规范化 payload 区分状态演进，不产生告警通知。
- Webhook 为至少一次投递，5 秒超时、指数退避、最多 8 次尝试；不跟随重定向。
  `Idempotency-Key` 是 `[connectionId,deviceId,eventId,transition]` JSON 数组的 SHA-256。
  接收方必须按该 key 去重；网络超时时接收方可能已经成功，不能承诺恰好一次。
- 图像分块保留持久 chunk ledger；重启后通过 SDK 重放完整 ledger，继续有界组装。
  SDK 校验分块范围、总长度、SHA-256、USTAR 安全成员，并 fsync 包与 receipt；之后才建立
  持久 `ackEvidencePackage` 任务。失败 ACK 重启后仍可重试，最多 8 次。
  REST `evidence/ack` 也要求本连接已验证同一个 eventId/hash。普通 JPEG 也持久保存。
- `maxEvidenceBytes` 是证据目录的保守准入预算：每个包预留两倍包大小用于 chunk ledger
  和组装文件，加上普通 JPEG。达到预算后停止准入并报告失败。已完成应用 ACK 的包和普通 JPEG 超过 `retentionDays` 后自动删除；
  包文件与 receipt、SQLite 容量记录一并回收。待 ACK、ACK 失败和未完成分块不会按时间删除。
- 定期按 `retentionDays` 清理已处理 inbox、历史事件、完成 outbox、已 ACK 证据和普通 JPEG；
  证据清理与组装/验证共用锁，不跟随符号链接，并校验路径限于证据目录。pending/failed 不会被
  保留策略删除。告警/证据状态事件历史分别最多保留 `10 * maxInboxRows` 行，超限先移除最旧记录，
  不影响活跃/有歧义的 lifecycle 或未完成任务。无歧义的已结束 lifecycle 在终止事件超过保留期，
  或补查已确认 inactive 且 `reconciledAt` 超过保留期后清理；active 状态始终保留。
  去重窗口受时间与容量共同限制，超出窗口没有永久去重保证。

`/health` 总是返回 200；断连或终止失败记录使 `status` 变为 `degraded`。`counters` 包含
`inbox_pending` / `inbox_failed`、`outbox_pending` / `outbox_failed`、`jobs_failed`、
`evidence_pending` / `evidence_failed` 等当前持久队列计数，及 `ingressFailures`、`mqttErrors`。
终止失败保留在 SQLite，记录异常类型，避免把 token/远端返回内容写入错误日志。
修复故障后，可在服务停止时检查相应行，将其 `status` 改回 `pending`、`attempts=0`、
`due=0` 后重启；不能直接丢弃未完成工作。`evidence` 的 `assembling` 记录不能改成 pending：
必须收到并校验完整包才可确认。需要保留超过自动清理窗口的证据时，请在到期前归档。自动清理完成后不能再通过 REST 确认已删除包。

## 验证与范围

```bash
cd python
.venv/bin/python -m unittest discover -s tests -v
```

测试包括全部 33 个请求的 JSON 与 Protobuf 黄金字节对照、连接隔离、告警去重/相同时间
补查、事务回滚、webhook 重启与有界重试、证据分块重启与 ACK 重试、鉴权、HTTP 参数边界、
MQTT 提交后 ACK 顺序和 RPC 总超时预算。直接依赖在 `requirements.txt`，完整传递依赖锁在 `requirements.lock`，由隔离的 Python 3.12 服务镜像解析并在 Python 3.11.7 / 3.12 验证；新增依赖版本
查验来源：[FastAPI 0.115.12](https://pypi.org/project/fastapi/0.115.12/)、
[Uvicorn 0.34.2](https://pypi.org/project/uvicorn/0.34.2/)、
[HTTPX 0.28.1](https://pypi.org/project/httpx/0.28.1/)。

这是单实例接入参考实现：没有集群协调、跨实例共享 pending、自动数据库升级到未知版本、
历史遥测库或告警规则引擎。进程/磁盘丢失、Broker 不保留持久会话、QoS 0 丢包不会由服务
自动修复；真实硬件能力与固件行为需要现场验证。33 个方法的编码支持不等于硬件实测覆盖。
