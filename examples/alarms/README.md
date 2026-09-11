# 告警配置与订阅

各语言客户端使用同一组 [请求示例](requests.json) 和 [告警消息](events.json)，支持 Protobuf（推荐）与 JSON。默认只查询能力；加 `--print-only` 可查看请求并验证编码，不连接设备。

| 内容 | 示例 |
| --- | --- |
| 能力、规则、当前告警、历史 | `capabilities`、`list-rules`、`active-state`、`history`、`incident-history` |
| 新建五种规则 | `create-displacement`、`create-rate`、`create-target-lost`、`create-input-voltage`、`create-battery-voltage` |
| 完整更新并禁用、删除 | `update-disabled`、`delete-rule` |
| 六种状态变化 | [events.json](events.json)：触发、升级、降级、同步、恢复、取消 |

## 运行

示例运行在系统集成商的电脑或服务器上，与设备连接同一个 MQTT Broker。首次接入请先按[位移数据入门](../../README.md#receive-data)接收监测数据，再继续本页的告警操作。下列占位值需替换为实际参数；`VDM_PAYLOAD_FORMAT` 必须与设备该连接的格式一致。

在终端设置参数：

```bash
export MQTT_HOST=YOUR_BROKER_HOST  # 系统集成商提供的域名或 IP
export MQTT_PORT=1883              # 替换为系统集成商指定的端口
export VDM_DEVICE_ID=YOUR_DEVICE_ID # 要访问的设备 ID
export VDM_PAYLOAD_FORMAT=protobuf  # JSON 连接改为 json
# 按系统集成商要求设置 MQTT_USERNAME / MQTT_PASSWORD
```

连接本机 NanoMQ 时，设置 `MQTT_HOST=127.0.0.1`、`MQTT_PORT=18883`。选择一种语言；Python 复用位移入门的环境，其他语言从仓库根目录开始。最后一条命令查询设备能力，成功时打印 `RESPONSE`。尚未准备好设备时，加 `--print-only` 即可离线检查编码。

后续 `--case`、`--request` 等选项均替换所选语言运行命令的参数，并在对应语言目录执行。例如将 `--case capabilities` 换成 `--case active-state --listen 60`，可查询当前告警并继续接收 60 秒的 `3A` / `event` 消息。

### Python

[完整代码](../../python/alarm_rpc.py)。先完成[Python 准备与连接参数设置](../../README.md#python-setup)，在同一终端、`python/` 目录执行：

```bash
python alarm_rpc.py --case capabilities
```

### JavaScript

[完整代码](../../javascript/alarm_rpc.js)，要求 Node.js 22+。

```bash
cd javascript
npm ci
node alarm_rpc.js --case capabilities
```

### Go

[完整代码](../../go/cmd/alarm-rpc/main.go)，要求 Go 1.24+ 和 `protoc`。

```bash
cd go
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p generated
protoc -I../proto --go_out=generated --go_opt=paths=source_relative ../proto/inteagle_vdm_mqtt_v1.proto
go run ./cmd/alarm-rpc --case capabilities
```

### Java

[完整代码](../../java/src/main/java/com/inteagle/examples/vdm/AlarmRpc.java)，要求 JDK 21+ 和 Maven。

```bash
cd java
mvn -q package
java -cp target/vdm-mqtt-consumer-1.0.0.jar com.inteagle.examples.vdm.AlarmRpc --case capabilities
```

## 配置步骤

1. 查询 `getAlarmCaps`，按设备返回的等级、规则类型和动作能力提供配置选项；用 `getTargets` 获取实际标靶 ID。
2. 查询 `listAlarmRules`。选择 [requests.json](requests.json) 中对应格式的参数，替换标靶 ID，保存到所选语言目录的 `request.json`：`{"method":"applyAlarmRules","params":{...}}`。
3. 先执行 `--request request.json --print-only` 检查编码，再去掉 `--print-only` 提交。编码通过不代表设备业务校验通过，以 RPC 响应为准。
4. 新建后保存返回的 `createdIds`，再查询 `listAlarmRules` 核对。更新须提交完整规则，不能只发 `id` 和待改字段；禁用时保留完整配置并设 `enabled:false`。
5. 删除使用 `deleteIds:[实际规则ID]`；响应成功后再次查询确认。

新建示例均为 `enabled:false`，方便先回读检查。需要参与告警判定时，提交完整规则并改为 `true`。示例中的 `T01`、更新/删除用的 `12` 均须替换为实际值。

## 字段与格式

`requests.json` 中 `json` 是直接 MQTT JSON 的 `params`；`protobuf` 是交给 SDK 编码的 Protobuf 字段视图，在线发送的是二进制。

| 含义 | Protobuf 字段视图 | MQTT JSON |
| --- | --- | --- |
| 位移越限规则 | `upsert:[{"displacementLimit":{...}}]` | `type:"DISP_LIMIT"` |
| X 方向、双向判定 | `metric:"ALARM_METRIC_DX"`、`direction:"DISPLACEMENT_DIRECTION_BIDIRECTIONAL"` | `metric:"DX"`、`direction:"BIDIRECTIONAL"` |
| ALARM 阈值 | `levels:{"alarm":{"enter":10,"enterForMs":1000}}` | `levels:{"ALARM":{"enter":10,"enterForMs":1000}}` |
| 抓拍动作 | `actionType:"ALARM_ACTION_TYPE_SNAPSHOT"` | `type:"SNAPSHOT"` |

等级顺序为 `ALERT < ALARM < ACTION`，规则可只启用其中部分等级。位移单位 mm，电压单位 V。速率短窗口使用 mm/s，`windowMs=3600000` 使用 mm/h，`86400000` 使用 mm/day（本仓库速率示例为 3000 ms、mm/s）；规则持续时间和历史记录时间戳单位 ms，`3A` 时间戳单位秒。

以 `deviceId + alarmId` 关联生命周期，以 `deviceId + eventId` 去重状态变化。`RECOVERED`、`CANCELLED` 不携带 `level`；不能把缺失等级当成 `ALERT`。`alarmId`、`eventId` 在 JSON 和 SDK 的 Protobuf 可读视图中使用十进制字符串，避免 JavaScript 大整数精度丢失。规则 `id` / `ruleId` 使用数值。

## 告警抓拍上传

仅配置一个已启用的系统集成商 MQTT 连接，且未另行指定或关闭抓拍上传时，设备默认向该连接发送抓拍。规则必须配置 `SNAPSHOT` 动作；未配置动作时只产生告警。多个系统集成商 MQTT 连接时，需要指定接收目标。速率告警抓拍仅支持 1000～10000 ms 短窗口。

系统集成商平台订阅 `vdm/{deviceId}/image`，接收类型 2 的二进制图像包分块；该格式不受 JSON / Protobuf 选择影响。校验并保存完整包后，必须从接收图像的同一 MQTT 连接调用 `ackEvidencePackage`；Broker 的 PUBACK 不代表系统集成商平台已完成接收。以 `eventId` 关联 `3A` 告警。

[查看各语言抓拍接收、落盘与确认代码及运行命令](../../README.md#alarm-evidence)。

需要补传时，保持接收器运行。在另一终端设置相同连接参数，进入所选语言目录，把以下请求保存为 `retry.json`，再用该语言运行命令执行 `--request retry.json`（两种格式均适用，替换实际 `eventId`）：

```json
{"method":"retryEvidence","params":{"eventId":"9001"}}
```

方法改为 `getEvidenceStatus` 可查询进度。补传请求成功只表示设备已受理，仍需接收、校验和确认图像包；接收器打印 `ACKED` 才表示确认 RPC 成功。

## 实机验证

按[启动步骤](../../README.md#quick-test)让设备和各语言接收器接入系统集成商 MQTT Broker，先确认收到位移。需要自建测试 Broker 时，可使用[附带的 NanoMQ](../../README.md#nanomq-local)。随后用本页命令查询能力、配置规则和接收告警；抓拍接收、保存及确认见[抓拍接入说明](../../README.md#alarm-evidence)。分别选择设备支持的 JSON 或 Protobuf 完成接入。
