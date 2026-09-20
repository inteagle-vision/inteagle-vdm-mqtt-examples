# 接口覆盖基线

来源：固件 `backend_service/src/application/cloud/protocol/vdm_rpc.rs`、`vdm_protobuf.rs` 和 `scripts/test/vdm_mqtt_protobuf_nanomq/public_rpc_matrix.json`。Schema SHA256 见 `proto/SHA256SUMS`。

SDK 与 REST 契约覆盖不等于实机支持；型号能力以固件矩阵为基线，现场配置与 firmware 返回码为最终依据。

| RPC | REST 后缀 | Python / Go / Java / JavaScript | 编码 | 型号限制 |
|---|---|---|---|---|
| `getAttr` | `device/attributes/query` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `setAttr` | `device/attributes/update` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `reboot` | `device/reboot` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `syncTime` | `device/time/sync` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getTargets` | `targets/query` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `addTargets` | `targets/add` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `setTargets` | `targets/update` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `deleteTargets` | `targets/delete` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `initRefTargets` | `targets/initialize` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `startMeasurement` | `measurement/start` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `stopMeasurement` | `measurement/stop` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `syncTelemetry` | `measurement/sync` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getSyncStatus` | `measurement/sync/status` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `cancelSync` | `measurement/sync/cancel` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getLightLevel` | `device/lights/query` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `setLightLevel` | `device/lights/update` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `snapshot` | `evidence/snapshot` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `ispCtl` | `device/isp` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getMotorAngle` | `device/motor/angle/query` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `setMotorAngle` | `device/motor/angle/update` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `setMotorZero` | `device/motor/zero` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `enableMotor` | `device/motor/enable` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `disableMotor` | `device/motor/disable` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `getCruisePaths` | `device/cruise/query` | 四语言具名接口 | JSON / Protobuf | 需 motor 能力 |
| `getAlarmCaps` | `alarms/capabilities` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `listAlarmRules` | `alarms/rules/query` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `applyAlarmRules` | `alarms/rules/apply` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getAlarmState` | `alarms/state` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `listAlarmHistory` | `alarms/history` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `listAlarmEvents` | `alarms/events` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `getEvidenceStatus` | `evidence/status` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `retryEvidence` | `evidence/retry` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |
| `ackEvidencePackage` | `evidence/ack` | 四语言具名接口 | JSON / Protobuf | 通用公开边界；设备可拒绝不支持参数 |

## 型号矩阵

| 型号 | motor | 硬件验收 |
|---|---|---|
| v1 | 不支持 | 契约测试；未逐型号实机验证 |
| v1-pro | 不支持 | 契约测试；未逐型号实机验证 |
| v1-lite | 不支持 | 契约测试；未逐型号实机验证 |
| v2-h | 不支持 | 契约测试；未逐型号实机验证 |
| v2-w | 不支持 | 契约测试；未逐型号实机验证 |
| v2-k | 不支持 | 契约测试；未逐型号实机验证 |
| o1 | 支持 | 契约测试；未逐型号实机验证 |
| o2-h | 支持 | 契约测试；未逐型号实机验证 |
| x1 | 支持 | 契约测试；未逐型号实机验证 |
| x2-h | 支持 | 契约测试；未逐型号实机验证 |

该矩阵没有公布的型号（例如其他 DM610/DM500 标识）不推断为上表型号；先查询设备属性并由供应方确认能力。`capabilities` 配置是部署者的已验证声明，不由型号字符串猜测。

## 已保留但不可用的方法

`getStorageInfo`, `queryTelemetry`, `uploadS3`, `setCruisePoint`, `removeCruisePoint`, `startPatrol`, `stopPatrol`, `getPatrolStatus`：固件有文档/保留字段，但没有公开兼容处理器。四语言 SDK 拒绝调用，不提供 REST 路由。

## 编码差异

- JSON `setAttr.sampleFreq` ↔ ProtoJSON `sampleFrequencyHz`。
- JSON `setLightLevel.level` ↔ ProtoJSON `allLightsLevel`。
- JSON motor `pan/tilt/speed` ↔ ProtoJSON `panDegrees/tiltDegrees/speedPercent`。
- 告警规则枚举、oneof 和对象结构见 `examples/alarms/requests.json`，不得机械换大小写。
- Protobuf 的 uint64 统一使用十进制字符串。普通图像/图像包在两种编码连接中均为二进制。
- `listAlarmEvents` 分页字段和遥测补传接口已按当前固件同步。

实机记录见 [acceptance.md](acceptance.md)。
