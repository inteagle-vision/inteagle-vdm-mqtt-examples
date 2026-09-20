# JavaScript 完整服务

Node 20.19+（推荐 22），在本目录执行 `npm ci`，然后：

```sh
export VDM_SERVICE_CONFIG=../contracts/config.example.json
export VDM_DATA_DIR=./data
npm run service
```

默认监听 `127.0.0.1:8080`。编辑配置中的 Broker 与设备 ID，凭据使用
`usernameEnv` / `passwordEnv` 引用环境变量。一个连接可列出多个设备。
服务启动时 Broker 不在线仍提供 `/health` 并后台重连。
完整路由与响应见 [共同契约](../contracts/openapi.json)。

```sh
curl -X POST http://127.0.0.1:8080/v1/connections/local/devices/DEMO/device/attributes/query \
  -H 'Content-Type: application/json' -d '{"params":{}}'
curl http://127.0.0.1:8080/v1/connections/local/devices/DEMO/latest
```

`202` 表示设备已受理；用原有事件/查询核对结果。`504` 表示执行结果未知，
修改操作须先回读，服务不会自动重发。带参数的请求按连接 format 使用 JSON
或 ProtoJSON；两者有不同字段/枚举，见原有告警示例。

`VDM_API_TOKEN` 启用 Bearer 鉴权，远程监听必须设置；远程部署应配 TLS 反向代理。
`VDM_WEBHOOK_URL` 可配置接收告警通知的 HTTP(S) 服务，`VDM_WEBHOOK_TOKEN` 为可选
Bearer Token。通知携带稳定 `Idempotency-Key`，下游必须去重；任务最多重试 8 次。
未配置 URL 时通知任务保留，容量耗尽会反映为接收失败，需设置 URL 后继续处理。

SQLite WAL/FULL 单实例；目录锁防止两个服务打开同一目录。退出释放锁，异常
退出后锁最多约 10 秒过期。不要让其他语言实例共享此目录。备份目录应包含
SQLite 及 WAL、证据文件。持久化成功后才确认 QoS1；QoS0 仅尽力接收。
图像哈希/USTAR 校验运行在单独 worker，分块存入 SQLite 后可以重启重建。
普通 JPEG 与告警 TAR 都有容量上限；只有已验证/落盘证据才安排应用层 ACK。

```sh
npm run check
# 仓库 tests/run_services.sh 会启动独立测试 Broker，不需要实机
```

SDK: `sdk/`；最小业务调用: `business_example.js`；HTTP 和存储/后台任务: `service/`。
原 `consumer.js`、`receive_data.js`、`alarm_rpc.js`、`evidence_receiver.js` 保留。
