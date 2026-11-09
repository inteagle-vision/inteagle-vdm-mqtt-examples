# 可选：SQLite 位移历史

SQLite 仅属于 example，不进入 SDK / Starter 的依赖树。系统已有数据库时，可在监听器中将数据写入现有存储。

## 运行与查询

配置连接后，在本地配置设置 `demo.storage.enabled=true` 后运行 `VDM Example`，或：

```bash
./java/run-spring-boot.sh --demo.storage.enabled=true
```

默认数据库为仓库工作目录的 `vdm.sqlite3`，可通过 `--demo.database=/path/vdm.sqlite3` 修改。无需手工建表。

[VdmBusinessExample](../example/src/main/java/com/inteagle/examples/boot/VdmBusinessExample.java) 调用 [DisplacementStore](../example/src/main/java/com/inteagle/examples/vdm/DisplacementStore.java)，把 JSON / Protobuf 位移批次展开为时序记录；缺失方向保存为 NULL，重复上报按主键去重。

查询指定设备、标靶和时间段，时间使用 Unix 毫秒，区间为 `[from, to)`：

```bash
./java/run-example.sh DisplacementHistory ./vdm.sqlite3 DEVICE_A T01 1734567890000 1734567950000
```

## Schema 与维护

[独立 schema.sql](../example/src/main/resources/db/sqlite/schema.sql) 定义约束和索引：

| 表 | 用途 |
| --- | --- |
| schema_history | 迁移版本及 SQL 校验值 |
| displacement_samples | `(device_id, target_id, sample_time_ms)` 唯一的位移样本 |
| alarm_events | 设备告警事件去重 |
| incidents | 当前告警生命周期 |
| notification_outbox | 待发送通知及租约状态 |

[SqliteDatabase](../example/src/main/java/com/inteagle/examples/vdm/SqliteDatabase.java) 负责事务迁移、版本检查和旧版告警表迁移。使用 STRICT 表、外键、WAL、busy timeout；迁移失败整体回滚，拒绝未知高版本或已执行脚本校验不一致。不要直接修改已经使用的 V1 Schema，后续变更应新增迁移。

示例不自动删除历史数据。长期运行时，应按业务需求制定保留和备份策略；运行中的 WAL 数据库应使用 SQLite 一致性备份，不能只复制主文件。SQLite 的事务和去重只覆盖已经处理的数据，不等于 MQTT 到数据库的端到端可靠投递。
