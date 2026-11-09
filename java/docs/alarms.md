# 可选：告警与抓拍

接入告警功能前，先完成[连接参数](../README.md)，业务含义、规则字段和幂等策略见[公共告警文档](../../examples/alarms/README.md)。

CLI 进阶示例针对指定设备运行，先设置 `export VDM_DEVICE_ID=YOUR_DEVICE_ID`；多设备平台使用 Starter Gateway 路由。

## 查询与配置规则

```bash
./java/run-example.sh AlarmRpc --case capabilities
./java/run-example.sh AlarmRpc --case active-state --listen 60
./java/run-example.sh AlarmRpc --case create-displacement --print-only
```

默认推荐 Protobuf。没有设备时可用 `--print-only` 离线验证编码。内置请求打包在示例 JAR 的资源中，不依赖启动目录；`--request` 指定的文件按当前工作目录解析。

## 告警通知与 SQLite

Java 参考实现由
[AlarmNotificationConsumer](../example/src/main/java/com/inteagle/examples/vdm/AlarmNotificationConsumer.java)
和 [AlarmNotificationStore](../example/src/main/java/com/inteagle/examples/vdm/AlarmNotificationStore.java)
组成。前者负责 MQTT 接收，后者用 SQLite 事务实现 inbox、生命周期和 outbox；两种 Payload 格式解码后使用同一套策略。运行方式：

```bash
mvn -f java/pom.xml -pl example -am package
java -cp java/example/target/vdm-mqtt-example-1.0.0-cli.jar \
  com.inteagle.examples.vdm.AlarmNotificationConsumer \
  --database ./alarm-notifications.sqlite3
```

日志中的 `action` 有三种：`NOTIFY` 表示通知已安全写入 outbox，`STATE_ONLY` 表示仅更新状态，`DUPLICATE` 表示已处理过。程序重启后仍使用同一个数据库，因此不会丢失去重记录。

### HTTP 通知发送示例

[NotificationOutboxWorker](../example/src/main/java/com/inteagle/examples/vdm/NotificationOutboxWorker.java)
是可直接运行的 outbox 消费者。它向 `NOTIFICATION_WEBHOOK_URL` 发送 `POST`，请求示例：

```http
POST /api/v1/vdm-alarm-notifications HTTP/1.1
Content-Type: application/json
Idempotency-Key: DEVICE001:9007199254740993:ALARM_TRIGGERED

{"deviceId":"DEVICE001","eventId":"9007199254740993","alarmId":"9007199254740900","notification":"ALARM_TRIGGERED"}
```

在另一个终端设置相同运行环境，进入仓库根目录，启动发送任务：

```bash
export NOTIFICATION_WEBHOOK_URL=https://notify.example.com/api/v1/vdm-alarm-notifications
java -cp java/example/target/vdm-mqtt-example-1.0.0-cli.jar \
  com.inteagle.examples.vdm.NotificationOutboxWorker \
  --database ./alarm-notifications.sqlite3
```

Worker 收到 `2xx` 后标记 `SENT`；网络错误和非 `2xx` 保留任务，30 秒后重试。`NOTIFY` 只代表通知任务已落库，不代表短信或电话已送达。

Webhook 接收端须先按 `Idempotency-Key` 持久化去重并可靠保存通知任务，再返回 `2xx`；不能先返回成功再异步落库。幂等保障还须延续到实际短信、电话或 App 发送端，防止“发送成功、尚未记录结果就崩溃”后的重试再次通知。

通知 Worker 与 MQTT Consumer 可部署为两个进程，使用同一个 SQLite 文件；量产平台建议把这三张表迁移到 MySQL/PostgreSQL，并沿用唯一键和 lease 语义。


建表与约束见 [schema.sql](../example/src/main/resources/db/sqlite/schema.sql)，版本与迁移说明见 [SQLite 示例](storage.md)。SQLite 不进入 Starter 的依赖树。

## 告警抓拍

```bash
export VDM_EVIDENCE_DIR=./evidence
./java/run-example.sh EvidenceReceiver
```

接收器按 `deviceId + eventId` 重组并持久化，依次输出 `CHUNK`、`VERIFIED`、`ACKED`。完整长度、SHA-256 与安全 USTAR 校验通过后，才使用接收图像的同一 MQTT 连接调用 `ackEvidencePackage`；Broker PUBACK 不能替代它。

ACK 限流、超时或临时断线按 1/2/4 秒退避，最多尝试 4 次；永久参数错误直接失败。普通 JPEG 接收不需要这套流程，见[图片订阅示例](../example/README.md#只配置需要的订阅)。

Starter 业务可直接使用 SDK 的 `AlarmSnapshotPackageAssembler`、`getEvidenceStatus`、`retryEvidence` 和 `ackEvidencePackage`，完整参考实现为 [EvidenceReceiver.java](../example/src/main/java/com/inteagle/examples/vdm/EvidenceReceiver.java)。
