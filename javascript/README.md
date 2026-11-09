# JavaScript / TypeScript 接入

要求 Node.js 22+。推荐 **Protobuf**，JSON 为兼容选项。本页独立说明本语言的安装与运行。

## 接收位移

从仓库根目录执行：

```bash
export MQTT_HOST=YOUR_BROKER_HOST
export MQTT_PORT=1883
export VDM_DEVICE_ID=YOUR_DEVICE_ID
export VDM_PAYLOAD_FORMAT=protobuf
# 按 Broker 要求设置 MQTT_USERNAME / MQTT_PASSWORD
cd javascript
npm ci
node receive_data.js
```

[完整接收代码](receive_data.js)。日志出现标靶及位移数组才表示已收到测量数据；仅 READY 表示订阅完成。位移单位 mm。

## 按需继续

- 查询属性：在同一目录运行 `node receive_data.js --query-attributes`。
- [告警配置、抓拍与确认](docs/alarms.md)。
- [公共 Topic 与协议](../docs/protocol.md)、[Docker 联调](../docs/docker.md)。

本轮 Java Starter 的自动生成流程不改变本语言的构建步骤，首次运行请完成上方准备命令。
