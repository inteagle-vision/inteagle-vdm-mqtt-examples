# Inteagle VDM MQTT 示例

系统集成商平台通过 MQTT Broker 接入设备。本仓库提供 Python、Go、Java 和 JavaScript 的数据接收、属性查询、告警配置及抓拍接收代码，支持 Protobuf（推荐）和 JSON。

1. [接入系统集成商 MQTT Broker](#quick-test)。
2. [接收位移](#receive-data)，再[查询设备属性](#query-attributes)。
3. [配置告警](examples/alarms/README.md)，接收并确认[告警抓拍](#alarm-evidence)。

<a id="quick-test"></a>

## 接入系统集成商 MQTT Broker

准备 Git、Docker Engine、Docker Compose v2，以及一台已配置标靶、完成初始化并启动测量的设备。

向系统集成商获取连接参数，在设备 App 中配置 MQTT 连接，并在运行示例的电脑或服务器上设置：

| 配置 | 填写内容 |
| --- | --- |
| Broker 地址 | 系统集成商提供的域名、公网 IP 或内网 IP |
| 端口 | 系统集成商指定的 MQTT 端口，以下以 `1883` 为例 |
| 用户名、密码 | 按系统集成商的认证要求填写 |
| 设备 ID | 需要接收数据的设备 ID |
| Payload | `Protobuf`（推荐）或 `JSON` |

```bash
git clone https://github.com/inteagle-vision/inteagle-vdm-mqtt-examples.git
cd inteagle-vdm-mqtt-examples
export MQTT_HOST=YOUR_BROKER_HOST
export MQTT_PORT=1883              # 替换为实际端口
export VDM_DEVICE_ID=YOUR_DEVICE_ID
export VDM_PAYLOAD_FORMAT=protobuf # JSON 使用 json
# Broker 要求认证时，设置以下参数：
# export MQTT_USERNAME='YOUR_USERNAME'
# export MQTT_PASSWORD='YOUR_PASSWORD'
./run_demo.sh "$VDM_PAYLOAD_FORMAT"
```

接收端账号须有设备 Topic 的订阅权限，以及 RPC 请求 Topic 的发布权限。脚本在后台启动接收程序，连接上述 Broker。查看日志：

```bash
docker compose logs -f python-data go-data java-data javascript-data
```

按 `Ctrl-C` 退出日志查看，服务继续运行。`READY` 表示完成订阅；日志出现设备的标靶 ID 和位移数组，才表示收到测量数据。位移单位为 mm：

| 格式 | 可读输出中的位移字段 |
| --- | --- |
| Protobuf | `displacement.targets[].dx`、`dy` |
| JSON | `disp.d[标靶ID].dx`、`dy` |

<a id="nanomq-local"></a>

## 可选：启动 NanoMQ 联调

需要自建 Broker 时，在仓库根目录执行，沿用设备 ID 和数据格式：

```bash
unset MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD
export NANOMQ_BIND_ADDRESS=0.0.0.0
export NANOMQ_PORT=18883
./run_demo.sh "$VDM_PAYLOAD_FORMAT"
```

脚本启动附带的 NanoMQ 和各语言接收端。在设备 App 中填写设备可达的服务器域名或 IP、映射端口（默认 `18883`）及对应 Payload 格式。此 NanoMQ 联调配置默认未启用用户名密码认证。

<a id="receive-data"></a>

## 单独运行一种语言

也可以使用本机语言环境运行。沿用系统集成商提供的连接参数，再选择语言：

```bash
export MQTT_HOST=YOUR_BROKER_HOST
export MQTT_PORT=1883
export VDM_DEVICE_ID=YOUR_DEVICE_ID
export VDM_PAYLOAD_FORMAT=protobuf # JSON 连接使用 json
# Broker 要求认证时，设置 MQTT_USERNAME 和 MQTT_PASSWORD
```

连接本机 NanoMQ 时，使用 `MQTT_HOST=127.0.0.1 MQTT_PORT=18883`。以下安装命令从仓库根目录执行。

<a id="python-setup"></a>
<a id="python-sdk"></a>

### Python

要求 Python 3.10+。

```bash
cd python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m grpc_tools.protoc -I../proto --python_out=. ../proto/inteagle_vdm_mqtt_v1.proto
python receive_data.py
```

[接收代码](python/receive_data.py) · [SDK](python/vdm_mqtt_sdk)

<a id="go-sdk"></a>

### Go

要求 Go 1.24+ 和 `protoc`。

```bash
cd go
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p generated
protoc -I../proto --go_out=generated --go_opt=paths=source_relative ../proto/inteagle_vdm_mqtt_v1.proto
go run ./cmd/receive-data
```

[接收代码](go/cmd/receive-data/main.go) · [SDK](go/sdk)

<a id="java-sdk"></a>

### Java

要求 JDK 21+ 和 Maven；构建会生成 Protobuf 类型并打包依赖。

```bash
cd java
mvn -q package
java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.ReceiveData
```

[接收代码](java/src/main/java/com/inteagle/examples/vdm/ReceiveData.java) · [SDK](java/src/main/java/com/inteagle/vdm/mqtt/sdk)

<a id="javascript-sdk"></a>

### JavaScript / TypeScript

要求 Node.js 22+。

```bash
cd javascript
npm ci
node receive_data.js
```

[接收代码](javascript/receive_data.js) · [SDK](javascript/sdk) · [TypeScript 类型](javascript/sdk/index.d.ts)

各接收入口默认订阅 `telemetry`、`attributes`，持续输出设备数据。按 `Ctrl-C` 退出。

<a id="query-attributes"></a>

## 查询设备属性

使用 Docker 联调时，在仓库根目录运行下列任一命令，保留启动时导出的环境变量：

```bash
docker compose run --rm --no-deps python-data --query-attributes
docker compose run --rm --no-deps go-data --query-attributes
docker compose run --rm --no-deps java-data --query-attributes
docker compose run --rm --no-deps javascript-data --query-attributes
```

每条命令查询后持续接收数据，按 `Ctrl-C` 退出，再运行下一种语言。

使用本机语言环境时，停止上面的接收进程后，在相同语言目录、沿用相同连接参数运行：

| 语言 | 命令 |
| --- | --- |
| Python | `python receive_data.py --query-attributes` |
| Go | `go run ./cmd/receive-data --query-attributes` |
| Java | `java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.ReceiveData --query-attributes` |
| JavaScript | `node receive_data.js --query-attributes` |

程序调用一次 `getAttr`，查询设备 ID、型号、固件版本和测量状态，打印 `RESPONSE` 后继续接收数据。`code:0` 表示成功。

## 告警配置与订阅

按 [各语言告警示例](examples/alarms/README.md) 查询能力和规则，再提交自己的完整配置。示例包括五种规则、三级告警、规则增删改以及当前告警和历史查询。

| 语言 | 查询当前告警并监听 60 秒 |
| --- | --- |
| Python | `python alarm_rpc.py --case active-state --listen 60` |
| Go | `go run ./cmd/alarm-rpc --case active-state --listen 60` |
| Java | `java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.AlarmRpc --case active-state --listen 60` |
| JavaScript | `node alarm_rpc.js --case active-state --listen 60` |

`alarmId` 关联一次告警生命周期，`eventId` 关联其中一次状态变化。等级为 `ALERT < ALARM < ACTION`；`RECOVERED`、`CANCELLED` 结束生命周期且省略等级。

<a id="alarm-evidence"></a>

## 告警抓拍接收与确认

规则配置 `SNAPSHOT` 动作后才会抓拍。仅有一个已启用的系统集成商 MQTT 连接，且未另行指定或关闭抓拍上传时，设备默认向该连接发送图像；多个连接时需指定接收目标。

使用 Docker 时，在仓库根目录启动各语言的抓拍接收器：

```bash
./run_demo.sh "$VDM_PAYLOAD_FORMAT" --evidence
docker compose logs -f python-evidence go-evidence java-evidence javascript-evidence
```

每种语言使用独立的持久化卷保存图像包。使用本机语言环境时，在所选语言目录运行接收器；`VDM_EVIDENCE_DIR` 指定保存目录，每台设备使用独立目录：

```bash
export VDM_EVIDENCE_DIR=./evidence
```

| 语言 | 命令 | 完整代码 |
| --- | --- | --- |
| Python | `python evidence_receiver.py` | [Python](python/evidence_receiver.py) |
| Go | `go run ./cmd/evidence-receiver` | [Go](go/cmd/evidence-receiver/main.go) |
| Java | `java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.EvidenceReceiver` | [Java](java/src/main/java/com/inteagle/examples/vdm/EvidenceReceiver.java) |
| JavaScript | `node evidence_receiver.js` | [JavaScript](javascript/evidence_receiver.js) |

接收器使用 `deviceId + eventId` 关联告警和图像，依次输出：

- `CHUNK`：收到图像包分块。
- `VERIFIED`：完成长度、SHA-256 和安全 USTAR 校验，图像包与 `receipt.json` 已保存。
- `ACKED`：从接收图像的同一 MQTT 连接成功调用 `ackEvidencePackage`。

接收器遇到限流、超时或临时断线时，按 1、2、4 秒退避重试 ACK，最多尝试 4 次；参数错误直接报错。

`image` 始终使用二进制格式。类型 `1` 为普通 JPEG，类型 `2` 为 76 字节 Header 加最多 128 KiB 的告警图像包分块。Broker 的 PUBACK 不能替代应用层确认；需要补传时见 [查询进度与重试](examples/alarms/README.md#告警抓拍上传)。

<a id="topic-mapping"></a>

## Topic 与消息类型

| Topic | Protobuf 根消息 | 方向 |
| --- | --- | --- |
| `vdm/{deviceId}/telemetry` | `Telemetry` | 设备上报 |
| `vdm/{deviceId}/attributes` | `Attributes` | 设备上报 |
| `vdm/{deviceId}/event` | `Event` | 设备上报 |
| `vdm/{deviceId}/3A` | `Alarm` | 设备上报 |
| `vdm/{deviceId}/rpc/req` | `RpcRequest` | 平台下发 |
| `vdm/{deviceId}/rpc/resp` | `RpcResponse` | 设备响应 |
| `vdm/{deviceId}/image` | 二进制图片或图像包分块 | 设备上报 |

<a id="sdk"></a>

## SDK 能力

各语言均提供消息解析、29 个 RPC 的请求构造与响应关联、重连订阅、告警图像包重组与 ACK。配置项、枚举和请求参数见 [告警接入示例](examples/alarms/README.md)。

<a id="schema"></a>

## Schema

各语言共用 [inteagle_vdm_mqtt_v1.proto](proto/inteagle_vdm_mqtt_v1.proto)。声明了 `schema_version` 的消息，其值为 `1`。Protobuf 的 64 位 ID 在可读 JSON 输出中使用十进制字符串，保留完整精度。

```bash
sha256sum --check proto/SHA256SUMS
```

<a id="language-examples"></a>

## 代码入口

| 语言 | 接收位移 / 属性 | 告警配置 | 抓拍接收 |
| --- | --- | --- | --- |
| Python | [receive_data.py](python/receive_data.py) | [alarm_rpc.py](python/alarm_rpc.py) | [evidence_receiver.py](python/evidence_receiver.py) |
| Go | [receive-data](go/cmd/receive-data/main.go) | [alarm-rpc](go/cmd/alarm-rpc/main.go) | [evidence-receiver](go/cmd/evidence-receiver/main.go) |
| Java | [ReceiveData](java/src/main/java/com/inteagle/examples/vdm/ReceiveData.java) | [AlarmRpc](java/src/main/java/com/inteagle/examples/vdm/AlarmRpc.java) | [EvidenceReceiver](java/src/main/java/com/inteagle/examples/vdm/EvidenceReceiver.java) |
| JavaScript | [receive_data.js](javascript/receive_data.js) | [alarm_rpc.js](javascript/alarm_rpc.js) | [evidence_receiver.js](javascript/evidence_receiver.js) |

CI 回归测试保存在 [tests](tests) 与各语言测试目录，用于验证编码、边界和重组行为。设备接通以实际设备的 RPC 响应和数据上报为准。
