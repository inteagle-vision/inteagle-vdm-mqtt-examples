# Inteagle VDM MQTT 示例

本仓库提供 VDM 设备 MQTT 数据的 Python、Go、Java、JavaScript SDK 源码、完整解析示例和
NanoMQ 自动测试。四种语言使用同一份
[`proto/inteagle_vdm_mqtt_v1.proto`](proto/inteagle_vdm_mqtt_v1.proto)，可直接得到
遥测、设备属性、事件、告警、RPC 和图片 Header 的消息对象及可读字段。

设备连接建立时需要明确选择 `JSON` 或 `Protobuf` Payload 格式。云端应按照所选格式
解析，不应根据 Payload 字节自动猜测格式。`image` Topic 始终使用 VDM 图片 Header 与
JPEG 字节，不使用 JSON 或 Protobuf 包装。

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
topic=3A data={"schemaVersion":1,"eventId":"9001","alarmType":"ALARM_TYPE_DISPLACEMENT_LIMIT","level":"ALARM_LEVEL_ALERT","displacement":{"targetId":"T01","valueMm":3.5,"limitMm":3.0}}
```

当前 7 条示例消息的结构化内容完全对应：Protobuf 总计 `246` 字节，JSON 总计
`674` 字节，示例中减少约 `63.5%`。这只是本组测试数据的实测结果，实际节省比例取决于
位移点数、采样批次和可选字段；可以通过发布器输出的 `bytes` 对实际模型重新测量。

可通过环境变量修改宿主机映射端口：

```bash
NANOMQ_PORT=28883 ./run_demo.sh protobuf
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

四个订阅端都根据 Topic 选择明确的 Protobuf 根消息，并检查 `schema_version == 1`。
SDK 覆盖设备接入文档中的 29 个公开 RPC，并检查 `req_id` 和 Protobuf oneof 响应类型。

<a id="sdk"></a>

## SDK 能力

- 按连接配置解析 JSON 或 Protobuf，不猜测 Payload 格式。
- 返回语言对应的 Protobuf 消息对象，同时提供 JSON 兼容字段视图。
- 解析位移/环境量遥测、设备属性、事件、告警、RPC 以及图片 Header。
- 只允许文档公开的 29 个 RPC，并从普通字典/Map 构造对应的强类型 oneof。
- 自动维护并发 RPC 的 `req_id`、超时和错误码，断线重连后自动重新订阅。
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
│   └── publisher.py                     # JSON/Protobuf 测试数据发布
├── go/
│   ├── sdk/                             # Go SDK 与测试
│   └── cmd/consumer/main.go             # Go SDK 使用示例
├── java/src/
    ├── main/java/.../mqtt/sdk/           # Java SDK
    ├── main/java/.../Consumer.java       # Java SDK 使用示例
    └── test/java/.../VdmCodecTest.java   # Java SDK 单元测试
└── javascript/
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
