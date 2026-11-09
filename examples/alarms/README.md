# 告警接入：设备推送与平台通知

设备负责判定告警并推送消息，业务平台负责保存告警、去重，以及发送短信、电话或 App 推送。**仅接入告警通知时，订阅 `vdm/{deviceId}/3A` 即可；不要每收到一条 MQTT 消息就通知一次。**

## 先了解如何运行

1. 平台通过 RPC 配置并启用设备告警规则；配置成功后回读核对。
2. 设备自行判定阈值和持续时间，有状态变化时主动发布 `/3A`，不需要平台轮询触发。
3. 平台持久化去重、更新告警状态，再根据 `transition` 决定是否创建通知任务。
4. 后台任务调用通知服务；最终发送端也须做幂等处理，避免重试产生重复通知。

### 运行

选择自己的语言文档，仅运行所需功能：

- [Java 告警与抓拍](../../java/docs/alarms.md)
- [Python 告警与抓拍](../../python/docs/alarms.md)
- [Go 告警与抓拍](../../go/docs/alarms.md)
- [JavaScript 告警与抓拍](../../javascript/docs/alarms.md)

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

## 告警通知与防重复

`3A` 是告警状态变化流，不等于“每收到一条就通知一次”。MQTT QoS 1 是至少一次投递；网络断开时设备保留未获 Broker PUBACK 的告警，重连后重发，并用 `SYNCED` 同步仍然活动的告警。业务平台必须先持久化去重和更新告警生命周期，再异步发送短信、电话或 App 推送。

推荐的默认通知策略：

| `transition` | 平台状态处理 | 默认是否发送外部通知 | 说明 |
| --- | --- | --- | --- |
| `TRIGGERED` | 新建或打开 `alarmId` | 是 | 第一次进入告警等级 |
| `ESCALATED` | 更新活动等级 | 是 | 等级上升，例如 `ALERT → ALARM` |
| `DEESCALATED` | 更新活动等级 | 否 | 默认只更新页面；需要时可由项目配置通知 |
| `SYNCED` | 覆盖当前活动状态 | 否 | 连接恢复同步，不是新告警，禁止据此通知 |
| `RECOVERED` | 关闭 `alarmId` | 是，前提是平台已知该告警活动 | 恢复通知只发送一次 |
| `CANCELLED` | 关闭 `alarmId` | 是，前提是平台已知该告警活动 | 规则禁用、删除或状态取消等终态 |

生产实现至少需要三个持久化约束，不能只使用进程内 `set`：

1. 告警事件表以 `(device_id, event_id)` 为唯一键；冲突表示 QoS 1 或断线重放，直接返回成功，不重复执行业务副作用。
2. 告警生命周期表以 `(device_id, alarm_id)` 为唯一键；`SYNCED` 只修正当前状态，`RECOVERED/CANCELLED` 关闭生命周期。
3. 通知 outbox 以 `(device_id, event_id, notification_type)` 为唯一键，并与前两项在同一数据库事务中提交；事务提交后再由后台任务发送通知。

持久化与通知实现见[Java SQLite 示例](../../java/docs/alarms.md#告警通知与-sqlite)，轻量回放见[Python 示例](../../python/docs/alarms.md)。

按上述策略处理时，断线可能增加原始 MQTT 消息数量，但不会重复创建短信、电话或推送任务：同一 `eventId` 只处理一次，`SYNCED` 永不通知，同一终态也只会生成一个通知 outbox 记录。最终发送端也应使用上述幂等键；如果第三方通知接口不支持幂等，发送成功后、更新 outbox 前进程崩溃仍可能造成极少量重复，不能宣称严格的“绝对一次”。

## 告警抓拍上传

仅配置一个已启用的系统集成商 MQTT 连接，且未另行指定或关闭抓拍上传时，设备默认向该连接发送抓拍。规则必须配置 `SNAPSHOT` 动作；未配置动作时只产生告警。多个系统集成商 MQTT 连接时，需要指定接收目标。速率告警抓拍仅支持 1000～10000 ms 短窗口。

系统集成商平台订阅 `vdm/{deviceId}/image`，接收类型 2 的二进制图像包分块；该格式不受 JSON / Protobuf 选择影响。校验并保存完整包后，必须从接收图像的同一 MQTT 连接调用 `ackEvidencePackage`；Broker 的 PUBACK 不代表系统集成商平台已完成接收。以 `eventId` 关联 `3A` 告警。

[选择语言查看抓拍接收代码](../../README.md)。

需要补传时，保持接收器运行。在另一终端设置相同连接参数，进入所选语言目录，把以下请求保存为 `retry.json`，再用该语言运行命令执行 `--request retry.json`（两种格式均适用，替换实际 `eventId`）：

```json
{"method":"retryEvidence","params":{"eventId":"9001"}}
```

方法改为 `getEvidenceStatus` 可查询进度。补传请求成功只表示设备已受理，仍需接收、校验和确认图像包；接收器打印 `ACKED` 才表示确认 RPC 成功。

## 实机验证

按[启动步骤](../../docs/docker.md)让设备和各语言接收器接入系统集成商 MQTT Broker，先确认收到位移。需要自建测试 Broker 时，可使用[附带的 NanoMQ](../../docs/docker.md)。随后用本页命令查询能力、配置规则和接收告警；抓拍接收、保存及确认见[抓拍接入说明](../../docs/docker.md)。分别选择设备支持的 JSON 或 Protobuf 完成接入。
