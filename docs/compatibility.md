# 旧入口兼容

本次新增完整服务入口；现有脚本、参数、环境变量与 Java classpath jar 保留。
旧入口仍为单设备 SDK 示例，其告警通知/确认时机不冒充完整服务保证。

| 语言 | 保留入口 | 新完整服务 |
|---|---|---|
| Python | consumer.py, receive_data.py, alarm_rpc.py, alarm_notifications.py, evidence_receiver.py | `python -m vdm_service` |
| Go | cmd/consumer, cmd/receive-data, cmd/alarm-rpc, cmd/evidence-receiver | `go run ./cmd/service` |
| Java | Consumer, ReceiveData, AlarmRpc, AlarmNotificationConsumer, NotificationOutboxWorker, EvidenceReceiver | `java -jar target/vdm-mqtt-consumer-1.0.0-service.jar` |
| JavaScript | consumer.js, receive_data.js, alarm_rpc.js, evidence_receiver.js | `npm run service` |

`run_demo.sh` 和 `compose.yaml` 仍启动旧示例；完整服务使用独立
`compose.services.yaml`，避免更改已有部署的启动语义。
旧 Java `vdm-mqtt-consumer-1.0.0.jar` 仍可 `java -cp ... <MainClass>`。

SDK 增加四个公开 RPC；原公开方法和 wire 字段编号保持不变。新版 proto 新增
分页字段与 listAlarmEvents oneof，旧字段保留。自定义集成应重新生成绑定。
服务 SQLite schema version 1 是独立数据库，不直接读取/修改旧示例数据库。
不同语言不共享数据库文件，迁移时通过业务导出/重新同步；不承诺二进制数据库兼容。
