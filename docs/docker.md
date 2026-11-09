# Docker 联调

[返回语言导航](../README.md)。这是可选的多语言联合验证入口，只接入 Java 时请直接看 [Java 示例](../java/README.md)。


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


## 告警抓拍联调（可选）

```bash
./run_demo.sh "$VDM_PAYLOAD_FORMAT" --evidence
docker compose logs -f python-evidence go-evidence java-evidence javascript-evidence
```

每种语言使用独立持久化卷。抓拍业务与确认要求见[公共告警文档](../examples/alarms/README.md#告警抓拍上传)。
