# Python 告警与抓拍

先完成[本语言安装和连接配置](../README.md)，在 `python/` 目录执行。推荐 Protobuf。

```bash
python alarm_rpc.py --case capabilities
python alarm_rpc.py --case active-state --listen 60
python alarm_rpc.py --case create-displacement --print-only
```

不连接设备时可用 `--print-only`。规则配置步骤、字段与通知策略见[公共告警说明](../../examples/alarms/README.md)。

## 接收告警抓拍

```bash
export VDM_EVIDENCE_DIR=./evidence
python evidence_receiver.py
```

此处为告警类型 2 图像包，完整落盘校验后才 ACK，不等同于普通 JPEG。日志依次为 CHUNK、VERIFIED、ACKED。临时错误按 1/2/4 秒退避、最多尝试 4 次。

查询/补传：将公开请求保存为 `request.json`，使用 `python alarm_rpc.py --request request.json`。事件 ID 与补传格式见[抓拍协议](../../examples/alarms/README.md#告警抓拍上传)。

## SQLite 告警通知回放

```bash
python alarm_notifications.py --database ./alarm-notifications.sqlite3
```

这是本地回放演示，支持跨进程持久化去重，不向外部通知服务发送消息。
