# Broker 与协议

业务平台与设备连接同一个 MQTT Broker。准备 Broker 主机/端口、平台账号、设备 ID，并保证账号具备所选设备 Topic 的订阅权限；调用 RPC 还需要 `rpc/req` 发布权限。

推荐设备与平台均选择 **Protobuf**。JSON 仅作为兼容选项，收发双方配置必须一致。普通图片和告警证据包始终是二进制。


| Topic | Protobuf 根消息 | 方向 |
| --- | --- | --- |
| `vdm/{deviceId}/telemetry` | `Telemetry` | 设备上报 |
| `vdm/{deviceId}/attributes` | `Attributes` | 设备上报 |
| `vdm/{deviceId}/event` | `Event` | 设备上报 |
| `vdm/{deviceId}/3A` | `Alarm` | 设备上报 |
| `vdm/{deviceId}/rpc/req` | `RpcRequest` | 平台下发 |
| `vdm/{deviceId}/rpc/resp` | `RpcResponse` | 设备响应 |
| `vdm/{deviceId}/image` | 二进制图片或图像包分块 | 设备上报 |


## Schema

协议源文件为 [inteagle_vdm_mqtt_v1.proto](../proto/inteagle_vdm_mqtt_v1.proto)。声明了 `schema_version` 的消息其值为 1。64 位 ID 的可读 JSON 视图使用十进制字符串。

位移单位 mm。Protobuf 位移在 `displacement.targets[].dx/dy/dz`；JSON 在 `disp.d[标靶ID].dx/dy/dz`。每个数组是一批采样点，不是单个瞬时值。未上报的轴不等于零。

```bash
sha256sum --check proto/SHA256SUMS
```

告警和证据接入为可选能力，见[告警规则与幂等要求](../examples/alarms/README.md)。
