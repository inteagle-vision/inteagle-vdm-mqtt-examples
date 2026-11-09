# Go 接入

要求 Go 1.24+ 和 protoc。推荐 **Protobuf**，JSON 为兼容选项。本页独立说明本语言的安装与运行。

## 接收位移

从仓库根目录执行：

```bash
export MQTT_HOST=YOUR_BROKER_HOST
export MQTT_PORT=1883
export VDM_DEVICE_ID=YOUR_DEVICE_ID
export VDM_PAYLOAD_FORMAT=protobuf
# 按 Broker 要求设置 MQTT_USERNAME / MQTT_PASSWORD
cd go
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p generated
protoc -I../proto --go_out=generated --go_opt=paths=source_relative ../proto/inteagle_vdm_mqtt_v1.proto
go run ./cmd/receive-data
```

[完整接收代码](cmd/receive-data/main.go)。日志出现标靶及位移数组才表示已收到测量数据；仅 READY 表示订阅完成。位移单位 mm。

## 按需继续

- 查询属性：在同一目录运行 `go run ./cmd/receive-data --query-attributes`。
- [告警配置、抓拍与确认](docs/alarms.md)。
- [公共 Topic 与协议](../docs/protocol.md)、[Docker 联调](../docs/docker.md)。

本轮 Java Starter 的自动生成流程不改变本语言的构建步骤，首次运行请完成上方准备命令。
