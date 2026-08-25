# Inteagle VDM MQTT 示例

本仓库提供 VDM 设备 MQTT 数据的 Python、Go、Java、JavaScript SDK 源码、完整解析示例和
NanoMQ 自动测试。四种语言使用同一份
[`proto/inteagle_vdm_mqtt_v1.proto`](proto/inteagle_vdm_mqtt_v1.proto)，可直接得到
遥测、设备属性、事件、告警、RPC、普通图片和告警抓拍图像包的消息对象及可读字段。

设备连接建立时需要明确选择 `JSON` 或 `Protobuf` Payload 格式。云端应按照所选格式
解析，不应根据 Payload 字节自动猜测格式。`image` Topic 始终使用二进制结构：类型 `1`
是普通图片，类型 `2` 是 76 字节固定 Header + 最大 128 KiB USTAR 告警抓拍图像包分块；
二者均不使用 JSON 或 Protobuf 包装。

<a id="quick-test"></a>

## 一键验证

环境要求：Docker Engine 和 Docker Compose v2。

```bash
# 默认验证 Protobuf
./run_demo.sh

# 分别验证 Protobuf 或 JSON
./run_demo.sh protobuf
./run_demo.sh json

# 依次验证两种格式
./run_demo.sh all
```

脚本会构建并启动以下服务：

| 服务 | 作用 |
| --- | --- |
| `nanomq` | 本地 MQTT Broker，宿主机端口默认为 `18883` |
| `python-consumer` | Python Payload 解析示例 |
| `go-consumer` | Go Payload 解析示例 |
| `java-consumer` | Java Payload 解析示例 |
| `javascript-consumer` | JavaScript Payload 解析示例 |
| `publisher` | 等待四个订阅端就绪，发布测试数据并汇总结果 |

成功时四个语言订阅端会输出每条消息解析后的完整字段和 `PASS`，发布器最后输出
`PASS profile=... consumers=go,java,javascript,python`。任意语言解析失败、Topic 缺失或等待超时，
命令都会返回非零退出码。

例如 Protobuf 遥测和告警会解析为可读结构，而不是只打印 Payload 长度：

```text
topic=telemetry data={"schemaVersion":1,"displacement":{"sampleFrequencyHz":20,"targets":[{"targetId":"T01","dxMm":[0.125,0.25,0.375],"dyMm":[-0.5,-0.625,-0.75]}],"firstSampleTimestampMs":"1721805600000"}}
topic=3A data={"schemaVersion":1,"eventId":"9001","alarmId":"701","alarmType":"ALARM_TYPE_DISPLACEMENT_LIMIT","level":"ALARM_LEVEL_ALERT","transition":"ALARM_TRANSITION_TRIGGERED","displacement":{"targetId":"T01","valueMm":3.5,"limitMm":3.0}}
topic=image data={"messageType":2,"headerLength":76,"packageFormat":1,"evidenceKind":1,"eventId":"9001","packageLength":"10240","chunkIndex":0,"chunkCount":1}
```

示例消息同时验证 JSON/Protobuf 业务消息和二进制告警抓拍图像包分块。
发布器会输出每条消息的实际 `bytes`，可用实际标靶数和采样频率重新评估流量。

可通过环境变量修改宿主机映射端口：

```bash
NANOMQ_PORT=28883 ./run_demo.sh protobuf
```

默认只监听本机。如需让同一局域网中的真实设备连接测试 Broker，显式设置监听地址：

```bash
NANOMQ_BIND_ADDRESS=0.0.0.0 NANOMQ_PORT=18883 docker compose up nanomq
```

<a id="topic-mapping"></a>

## 示例覆盖范围

默认设备 ID 为 `DEMO001`，测试覆盖以下 Topic：

| Topic | Protobuf 根消息 | 方向 |
| --- | --- | --- |
| `vdm/DEMO001/telemetry` | `Telemetry` | 设备到云端 |
| `vdm/DEMO001/attributes` | `Attributes` | 设备到云端 |
| `vdm/DEMO001/event` | `Event` | 设备到云端 |
| `vdm/DEMO001/3A` | `Alarm` | 设备到云端 |
| `vdm/DEMO001/rpc/req` | `RpcRequest` | 云端到设备 |
| `vdm/DEMO001/rpc/resp` | `RpcResponse` | 设备到云端 |
| `vdm/DEMO001/image` | 非 Protobuf | 设备到云端 |

四个订阅端都根据 Topic 选择明确的 Protobuf 根消息。带 `schema_version`
的消息必须等于 `1`。SDK 覆盖 29 个强类型 Protobuf RPC，包括告警管理、
抓拍图像查询、重试和应用确认。

JSON Event 固定为 `{"type":"...","ts":<Unix秒>,"detail":{...}}`，
不接受旧的 `event/data` 别名。首发契约只有
`REF_INIT_RESULT`、`CRUISE_REACHED`、`TARGET_TRACKING` 和
`ALARM_EVIDENCE`。`TARGET_TRACKING.state` 只有 `LOST|TRACKING`；
配置规则产生的 `TARGET_LOST` 属于 `3A` 告警，不是普通 Event。

<a id="sdk"></a>

## SDK 能力

- 按连接配置解析 JSON 或 Protobuf，不猜测 Payload 格式。
- 返回语言对应的 Protobuf 消息对象，同时提供 JSON 兼容字段视图。
- 解析位移/环境量遥测、设备属性、事件、告警、RPC 以及两类图片 Header。
- 从字典/Map 构造 29 个强类型 Protobuf RPC；JSON 与 Protobuf 均支持 5 个告警管理 RPC。
- 四种语言均提供磁盘优先的有界分块重组、USTAR/长度/SHA-256/成员安全校验、原子接收回执，以及 `getEvidenceStatus`、`retryEvidence`、`ackEvidencePackage` 便捷调用。
- 自动维护并发 RPC 的 `req_id`、超时和错误码，断线重连后自动重新订阅。
- 四种语言都只以 `code == 0` 判断成功；`1` 是通用失败，其他非零码是可直接处理的细分错误。
- RPC 在线 Payload 不携带重复的 `msg/message` 文本；SDK 在本地按数字错误码生成可读说明。
- 每个 SDK 客户端实例拥有独立 Topic 和 pending 表；不同设备或云连接不能共享实例。

最后一条是 RPC 隔离边界：设备端按发起请求的 `cloudId` 返回响应，云端 SDK 再按当前
MQTT 客户端实例和 `req_id` 关联，不能把 A 连接的响应交给 B 连接。

<a id="python-sdk"></a>

### Python

[`python/vdm_mqtt_sdk`](python/vdm_mqtt_sdk) 是可直接复用的 SDK 包，
[`python/consumer.py`](python/consumer.py) 是完整运行示例。

```python
from vdm_mqtt_sdk import VdmMqttClient, VdmMqttClientConfig, VdmTopics

topics = VdmTopics.for_device("DEVICE_ID")

def on_message(message):
    print(message.suffix, message.as_dict())  # 完整可读字段
    telemetry = message.value                # Protobuf 模式下为强类型消息

with VdmMqttClient(
    VdmMqttClientConfig(
        host="mqtt.example.com",
        port=1883,
        topics=topics,
        payload_format="protobuf",
    ),
    on_message=on_message,
) as client:
    response = client.call("getAttr", {"keys": ["deviceId", "fwVer"]})
```

<a id="go-sdk"></a>

### Go

[`go/sdk`](go/sdk) 提供连接、编解码和 RPC 封装，
[`go/cmd/consumer`](go/cmd/consumer) 是完整运行示例。

```go
topics, _ := sdk.TopicsForDevice("DEVICE_ID")
client, _ := sdk.NewClient(sdk.Config{
    Host: "mqtt.example.com", Port: 1883, Topics: topics,
    PayloadFormat: sdk.Protobuf, QoS: 1,
}, func(message *sdk.DecodedPayload) {
    fields, _ := message.AsMap()
    fmt.Println(message.Suffix, fields)
}, func(err error) { log.Println(err) })

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = client.Start(ctx)
response, err := client.Call(
    ctx, "getAttr", map[string]any{"keys": []string{"deviceId"}}, 0, false,
)
```

<a id="java-sdk"></a>

### Java

[`java/src/main/java/com/inteagle/vdm/mqtt/sdk`](java/src/main/java/com/inteagle/vdm/mqtt/sdk)
提供 SDK，完整运行示例位于
[`Consumer.java`](java/src/main/java/com/inteagle/examples/vdm/Consumer.java)。

```java
VdmTopics topics = VdmTopics.forDevice("DEVICE_ID");
VdmMqttClient client = new VdmMqttClient(
    new VdmMqttClient.Config(
        "mqtt.example.com", 1883, topics, PayloadFormat.PROTOBUF,
        null, null, null, 1, Duration.ofSeconds(10)),
    message -> System.out.println(message.suffix() + " " + message.data()),
    Throwable::printStackTrace);

client.start();
DecodedPayload response = client.call(
    "getAttr", Map.of("keys", List.of("deviceId")), Duration.ofSeconds(10));
```

<a id="javascript-sdk"></a>

### JavaScript / TypeScript

[`javascript/sdk`](javascript/sdk) 是无需编译的 CommonJS SDK，并提供
[`index.d.ts`](javascript/sdk/index.d.ts) 类型声明；
[`javascript/consumer.js`](javascript/consumer.js) 是完整运行示例。

```javascript
const { VdmMqttClient, VdmTopics } = require("./sdk");

const client = new VdmMqttClient({
  host: "mqtt.example.com",
  port: 1883,
  topics: VdmTopics.forDevice("DEVICE_ID"),
  payloadFormat: "protobuf",
  qos: 1,
}, (message) => {
  console.log(message.suffix, message.data); // 完整可读字段
  console.log(message.value);                // protobufjs 消息对象
});

await client.start();
const response = await client.call(
  "getAttr",
  { keys: ["deviceId"] },
  { timeoutMs: 10_000 },
);
```

<a id="alarm-evidence"></a>

## 告警与抓拍图像对接

0.8.5 中，`alarmId` 表示一次完整告警生命周期，`eventId` 表示其中一次
状态变化。`RECOVERED` 或 `CANCELLED` 表示该 `alarmId` 的生命周期结束。
告警抓拍图像使用触发它的 `eventId`，因此客户平台的关联键为：

```text
deviceId + alarm.eventId == deviceId + imageHeader.eventId
```

建议平台保存 `deviceId + alarmId` 作为生命周期主键，并将 `eventId`
作为状态变化和抓拍图像去重键。不要从 ID 位布局互相推导。

StdMqtt 告警抓拍图像的流程为：

1. 接收 `3A` 告警，并在现有 `image` Topic 接收 `messageType=2` 的 128 KiB 图像包分块；两者可能乱序或重复。
2. 按 `deviceId + eventId + packageSha256 + chunkIndex` 幂等落盘，校验分块范围、图像包长度和 SHA-256。
3. 完整 USTAR 图像包持久化且验证 `manifest.json` 后，从同一 StdMqtt 连接调用 `ackEvidencePackage`。

四种语言使用同一接收边界：

| SDK | 分块重组器 |
| --- | --- |
| Python | `AlarmSnapshotPackageAssembler` |
| Go | `NewAlarmSnapshotPackageAssembler(...)` |
| Java | `AlarmSnapshotPackageAssembler` |
| JavaScript / TypeScript | `AlarmSnapshotPackageAssembler` |

重组器只有在图像包和 `receipt.json` 均持久化且校验通过后才返回完成结果；调用方随后从接收分块的同一 MQTT 客户端执行 ACK。

设备只启用一个 StdMqtt 连接时，0.8.5 默认选择该连接发送告警抓拍图像；若同时启用多个
StdMqtt，设备管理员必须明确选择一个抓拍图像目的地，固件不会猜测。客户平台无需接触
Inteagle OSS/STS 配置。

Python 参考接收器：

```bash
cd python
MQTT_HOST=mqtt.example.com \
MQTT_PORT=1883 \
MQTT_USERNAME=customer \
MQTT_PASSWORD='***' \
VDM_DEVICE_ID=DEVICE_ID \
VDM_PAYLOAD_FORMAT=json \
VDM_EVIDENCE_DIR=./evidence \
python evidence_receiver.py
```

它在 `<VDM_EVIDENCE_DIR>/<eventId>/` 下保存校验后的 USTAR 包和本地
`receipt.json`。`receipt.json` 是客户接收回执，不伪装成设备内部
`manifest.json`。示例不包含 Inteagle OSS/STS 凭据或内部上传接口。

告警配置和历史查询可以使用 JSON 或 Protobuf：

```javascript
const state = await client.call("getAlarmState", {});
const history = await client.call("listAlarmHistory", { limit: 20 });
const incident = await client.call("listAlarmHistory", { alarmId: "9754138318563442692" });
```

精确按 `alarmId` 查询 `listAlarmHistory` 时，设备会在该生命周期记录中
附加当前可见的抓拍图像摘要。Protobuf V1 已为
`getAlarmCaps` / `listAlarmRules` / `applyAlarmRules` / `getAlarmState` /
`listAlarmHistory` 提供强类型 oneof。

<a id="language-examples"></a>

## 目录结构

```text
.
├── compose.yaml                         # NanoMQ、四个消费者与发布器
├── proto/
│   ├── inteagle_vdm_mqtt_v1.proto       # V1 Schema（中文注释）
│   ├── inteagle_customer_mqtt_v1.desc    # V1 Protobuf Descriptor Set
│   └── SHA256SUMS                       # Schema 完整性校验
├── python/
│   ├── vdm_mqtt_sdk/                    # Python SDK
│   ├── tests/                           # Python SDK 单元测试
│   ├── consumer.py                      # Python SDK 使用示例
│   ├── evidence_receiver.py             # 抓拍图像包分块落盘、校验与 ACK 示例
│   └── publisher.py                     # JSON/Protobuf 测试数据发布
├── go/
│   ├── sdk/                             # Go SDK、图像包重组器与测试
│   └── cmd/consumer/main.go             # Go SDK 使用示例
├── java/src/
    ├── main/java/.../mqtt/sdk/           # Java SDK 与图像包重组器
    ├── main/java/.../Consumer.java       # Java SDK 使用示例
    └── test/java/.../VdmCodecTest.java   # Java SDK 单元测试
└── javascript/
    ├── sdk/snapshot-package.js           # JavaScript 图像包重组器
    ├── sdk/                              # JavaScript SDK 与 TypeScript 类型
    ├── tests/                            # Node.js SDK 单元测试
    └── consumer.js                       # JavaScript SDK 使用示例
```

Docker 构建期间，Python、Go、Java 从同一份 `.proto` 生成类型，JavaScript 由
`protobufjs` 直接加载同一 Schema，避免多份契约漂移。生产项目可按各语言目录中的依赖
与构建方式集成。Broker 地址、账号和设备 ID 均由 SDK 配置传入，
示例仓库不包含设备管理 HTTP 接口、内网地址或凭据。

可在仓库根目录校验 Schema 和 Descriptor Set 是否完整：

```bash
sha256sum --check proto/SHA256SUMS
```

<a id="schema"></a>

## Schema 兼容规则

- V1 Payload 的 `schema_version` 必须为 `1`。
- 已发布字段编号与含义不可修改；废弃字段应使用 `reserved` 保留。
- 新能力使用新的字段编号，并保持旧解析端能够忽略未知字段。
- RPC 的 `req_id` 必须原样返回；云端还必须按连接上下文隔离请求和响应。
- Payload 格式属于设备连接配置，不进行 JSON/Protobuf 自动探测。
- 位移空方向不产生编码字节；三个方向全空时省略该 target。云端使用批次的 `first_sample_timestamp_ms` 和 `sample_frequency_hz` 还原样本时间。

更完整的字段、RPC 方法和图片 Header 说明请参阅 VDM 设备接入文档。
