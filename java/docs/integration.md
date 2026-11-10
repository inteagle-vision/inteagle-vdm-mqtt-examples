# 系统集成说明

Starter 面向系统集成商接入 VDM 设备。默认推荐 Protobuf，JSON 为兼容模式；图片始终使用二进制。SDK 保留完整公开协议能力，按业务需求配置订阅范围。

## 按需订阅

`vdm.mqtt.subscriptions` 是 Topic 后缀列表，**省略时默认 `[telemetry]`**。设置 `all-devices: true` 时，`[telemetry, image]` 生成 `vdm/+/telemetry` 和 `vdm/+/image`；使用 `device-id` 或 `device-ids` 时，将 `+` 换成指定设备 ID。显式配置的列表替换默认值，不会自动追加 telemetry。

| 需求 | subscriptions |
| --- | --- |
| 位移（默认） | `[telemetry]` |
| 仅普通图片 | `[image]` |
| 仅命令查询 | `[rpc/resp]` |
| 位移和图片 | `[telemetry, image]` |
| 属性及告警 | `[attributes, "3A", event]` |
| 告警抓拍与确认 | `[image, event, rpc/resp]` |

MQTT Topic 区分大小写，告警后缀为 `3A`。以下从平台接入方的角度区分订阅与发布。

平台按需订阅的设备上行消息如下；完整 Topic 过滤器应配置在 `topic-filters` 中：

| 后缀 | 接收内容 |
| --- | --- |
| `telemetry` | 遥测，包含位移数据 |
| `image` | 图片 |
| `attributes` | 属性，包含标靶列表更新 |
| `event` | 事件 |
| `3A` | 告警 |
| `rpc/resp` | 设备 RPC 响应，主动调用 RPC 时需要订阅 |

`rpc/req` 是平台向设备**发布请求**的 Topic，不是平台调用 RPC 时需要订阅的消息；SDK 会发布到所选设备的 `vdm/{deviceId}/rpc/req`。平台账号需拥有该 Topic 的发布权限，以及 `rpc/resp` 的订阅权限。

协议层也允许显式订阅 `rpc/req` 以观察下行请求，或使用 `["#"]` 接收全部后缀；这些不是常规平台接入所需的订阅。`#` 会同时收到请求和设备上报，不能理解成“只接收所有设备数据”。

YAML 写作 `subscriptions: [telemetry, image, attributes]`，properties 写作 `vdm.mqtt.subscriptions=telemetry,image,attributes`，两者等价。不填写会使用默认值，显式填写空列表 `subscriptions: []` 则校验失败；希望先连接再动态订阅时使用 `topic-filters: []` 模式。

订阅只决定接收范围，不会主动发送查询或拍照 RPC，也不改变设备的 JSON/Protobuf 格式。示例组件提供位移、图片和标靶接收分支；事件、告警等可按需要在自己的 `VdmMessageListener` 中处理。修改文件需重启应用，运行时变更使用下文的 `subscribe/unsubscribe`。

## 单设备与多设备

使用 `VdmMqttGateway` 从 Topic 路由多设备；最小示例显式配置 `all-devices: true`。库本身不会在未配置设备范围时自动订阅全 Broker。有限多设备清单使用：

```yaml
vdm:
  mqtt:
    host: ${MQTT_HOST}
    device-ids: [DEVICE_A, DEVICE_B]
    payload-format: protobuf
    subscriptions: [telemetry, image, rpc/resp]
```

注入 `VdmMqttGateway`，通过 `gateway.device("DEVICE_A").getTargets()`、`requestSnapshot(...)` 或 `call(...)` 指定目标。各设备逻辑客户端共享一个物理连接，各有独立的 pending 请求表；`VdmMessageListener` 的第一个参数始终是实际设备 ID。

拥有全设备接入权限的平台账号可显式设置 `all-devices: true`，使用 `vdm/+/...` 订阅。`device-id`、`device-ids`、`all-devices`、`topic-filters` 四选一。订阅权限由 Broker ACL 控制，设备与租户的归属关系由应用管理。设备使用不同 Broker/认证/格式时，应配置独立 Gateway，不混用一套连接配置。

单设备兼容接法使用 `device-id`，可额外注入 `VdmMqttClient`；它是便捷入口，多设备接入使用 Gateway。

## 标准 MQTT 通配符与运行时订阅

订阅规则使用标准 MQTT Topic Filter：`+` 匹配一层，`#` 匹配零层或多层且必须放在末尾。比如 `vdm/#` 包含所有设备的所有消息，`vdm/+/rpc/+` 包含各设备的 RPC 请求和响应。SDK 只解释标准 VDM Topic，其他业务 Topic 由应用已有的 MQTT 消息处理器处理。

不同设备订阅不同类型时直接配置：

```yaml
vdm:
  mqtt:
    host: ${MQTT_HOST}
    topic-filters:
      - vdm/DEVICE_A/telemetry
      - vdm/DEVICE_B/image
      - vdm/+/attributes
      - vdm/+/rpc/resp
```

此模式仅订阅 `topic-filters`，不会再补上默认 telemetry。使用本仓库 example 时，将它的 `all-devices` 改成 `false`；不同时选择其他设备范围。

通配符范围内的新设备上线后自动接入，不需要注册设备 ID。指定设备模式则可以在业务服务中动态操作：

```java
// Gateway 已启动；每次调用等待 Broker 接受，不会新建 MQTT 连接。
gateway.subscribe("vdm/NEW_DEVICE/attributes");
gateway.subscribe("vdm/NEW_DEVICE/image");
gateway.subscribe("vdm/NEW_DEVICE/rpc/resp");
List<VdmTarget> targets = gateway.device("NEW_DEVICE").getTargets();
SnapshotReceipt receipt = gateway.device("NEW_DEVICE").requestSnapshot();
gateway.unsubscribe("vdm/NEW_DEVICE/image");
List<String> current = gateway.topicFilters();
```

也可以用 `topic-filters: []` 先连接，再由设备管理服务逐个增加过滤器。增加/取消操作从业务线程调用，不能在原生 MQTT 回调中同步等待；Starter 的监听器已经隔离线程。两种 Paho 适配器均实现运行时变更；自定义传输可实现 `subscribe/unsubscribe`，未实现时明确报不支持。

取消的是**原样相同的订阅过滤器**：如果仍有 `vdm/+/image`，取消 `vdm/NEW_DEVICE/image` 不会排除 NEW_DEVICE。重叠过滤器及 QoS 1 可能产生重复消息，持久化仍需业务去重。取消最后一个 rpc/resp 匹配过滤器后，该设备的等待中 RPC 会立即失败；已经投递到业务队列的消息仍可能处理完。

断线重连恢复当前成功订阅的列表；运行时列表仅保存在进程内，不修改配置文件。需要跨重启恢复动态设备清单时，从应用设备库重新加载订阅列表。订阅拒绝/超时会抛出异常；超时不能推断 Broker 从未执行，适配器会尝试清理或恢复订阅，恢复失败会报告错误。

完整配置项及中文提示由 Starter 配置元数据提供，也可查看 [VdmMqttProperties](../spring-boot-autoconfigure/src/main/java/com/inteagle/vdm/mqtt/spring/VdmMqttProperties.java)。默认只订阅 telemetry、QoS 1、连接超时 10 秒；失败会使 Spring 启动明确报错。

## 调用其他设备 RPC

| Topic | 消息方向 | 平台操作 |
| --- | --- | --- |
| `vdm/{deviceId}/rpc/req` | 平台 → 设备 | SDK 发布请求，无需订阅 |
| `vdm/{deviceId}/rpc/resp` | 设备 → 平台 | 订阅并由 SDK 关联响应 |

业务能力不限于示例展示的标靶和拍照。公开 RPC 统一通过指定设备的逻辑客户端调用：

```java
DecodedPayload response = gateway.device(deviceId)
    .call("getAttr", Map.of(), Duration.ofSeconds(10));
```

先订阅该设备的 `rpc/resp`；调用会等待匹配的响应，非成功返回码抛出 `RpcException`，超时不表示设备没有执行。`getTargets/requestSnapshot` 等专用接口封装常用业务；`call` 提供完整公开 RPC 能力。方法及参数以仓库 [Schema](../../proto/inteagle_vdm_mqtt_v1.proto) 和[公共协议](../../docs/protocol.md)为准，不调用设备内部方法。

## 复用已有的 Paho Bean

Starter 默认自动装配 `PahoMqttTransport`、`VdmMqttGateway` 和连接生命周期，无需手动创建 Paho 客户端。自动装配由本 Starter 提供，`vdm-mqtt-paho` 模块本身是传输适配库。默认 `vdm.mqtt.enabled=true`、`auto-startup=true`；设为 `enabled=false` 可关闭整套自动装配。

提供 `VdmMqttTransport` Bean 时默认 Paho 传输退让；提供自己的 `VdmMqttGateway` 或 `VdmMqttClient` Bean 时整套自动装配退让。仅有一个 `IMqttAsyncClient` Bean 不会自动被借用，需要显式包装如下。

通过 `SharedPahoMqttTransport` 显式接入已有连接，注册传输 Bean：

```java
@Bean(destroyMethod = "")
SharedPahoMqttTransport vdmTransport(
        @Qualifier("existingMqttClient") IMqttAsyncClient existingClient) {
    return new SharedPahoMqttTransport(existingClient);
}
```

`SharedPahoMqttTransport` 来自 `com.inteagle.vdm.mqtt.paho`，`IMqttAsyncClient` 来自 Paho。连接参数、TLS、认证、启动顺序和自动重连仍由原连接配置管理；此时不必再给 Starter 配 host/port。

在已有的全局回调中转发以下两个事件（通过 `ObjectProvider<SharedPahoMqttTransport>` 延迟获取可避免循环 Bean 依赖）：

```java
public void connectComplete(boolean reconnect, String serverURI) {
    transportProvider.ifAvailable(SharedPahoMqttTransport::connectionRestored);
}
public void connectionLost(Throwable cause) {
    transportProvider.ifAvailable(transport -> transport.connectionLost(cause));
}
```

适配器使用每个订阅的 `IMqttMessageListener`，不调用 `setCallback`，保留原全局回调，连接的建立、断开和关闭由原连接所有者负责。VDM 使用的 Topic 过滤器须由该适配器独占管理，不能与其他组件管理完全相同的过滤器。关闭时仅取消它管理的订阅；若共享连接离线且使用持久会话，原连接所有者还需在重连后清理遗留服务端订阅。

已有连接须在 Starter 生命周期启动前可用；若现有连接也是 `SmartLifecycle`，使用较早的 phase，或设置 `auto-startup: false`，待连接就绪后调用 `gateway.start()`。应用关闭时仍由 Starter 关闭自己的逻辑 Gateway。

## 替换 MQTT 实现

核心 SDK 只依赖 [VdmMqttTransport](../sdk/src/main/java/com/inteagle/vdm/mqtt/sdk/VdmMqttTransport.java)，不依赖 Paho 或 Spring。注册自己的 `VdmMqttTransport` Bean 即可让默认 Paho 配置退让，必要时从 Starter 依赖中排除 `com.inteagle.vdm:vdm-mqtt-paho`。

接口负责连接/订阅、发布、原始消息回调、断线通知和资源关闭；协议解析、设备路由、请求 ID 关联仍由 SDK 完成。自定义传输必须在 SUBACK 成功后返回启动成功，并在断线重连后恢复订阅。已有连接应使用“借用”语义，不关闭其他业务拥有的资源。

## 业务回调与持久化边界

Starter 根据设备 ID 分配到固定数量的业务分区，每个分区有一个有界串行队列；同设备保持接收顺序，不同分区可并行，支持 `@Order`。默认 `listener-concurrency: 4`、每分区 `listener-queue-capacity: 256`。同一个监听器 Bean 可被不同分区同时调用，共享状态须保证线程安全。散列到同一分区的设备仍会互相等待，可按吞吐量调整分区数。监听器可调用同步 RPC，一个监听器抛异常不会阻止其余监听器。SDK 协议层本身不依赖线程池，直接使用 SDK 时回调不要阻塞等待 RPC。

默认使用 Spring `ThreadPoolTaskExecutor`，兼容 JDK 17。JDK 21+ 设置 `spring.threads.virtual.enabled: true` 后使用 Spring `SimpleAsyncTaskExecutor` 虚拟线程；分区数、顺序和队列上限仍然生效，并非无限并发。JDK 17 开启此开关会明确报错。

提供名为 `vdmMqttTaskExecutor` 的 Bean 可替换默认执行器；该专用配置与通用的 `applicationTaskExecutor` 分开：

```java
@Bean(name = "vdmMqttTaskExecutor", defaultCandidate = false)
ThreadPoolTaskExecutor vdmMqttTaskExecutor() {
    var executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(4);
    executor.setMaxPoolSize(4);
    executor.setQueueCapacity(4);
    executor.setThreadNamePrefix("app-vdm-");
    executor.setWaitForTasksToCompleteOnShutdown(true);
    executor.setAwaitTerminationSeconds(5);
    return executor; // Spring 负责初始化和销毁，无需手动 initialize。
}
```

类型来自 `org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor`，也接受其他 `java.util.concurrent.Executor` 实现。`defaultCandidate = false` 防止专用执行器影响 Spring Boot 对通用执行器的自动配置。执行器必须异步执行、不能静默丢弃任务；容量不足应抛出拒绝异常，不能使用 `CallerRunsPolicy` 或 `DiscardPolicy`。默认执行器由 Starter 管理；自定义执行器由应用管理生命周期，Starter 仅提交任务。不要在 MQTT 接收线程中使用会阻塞等待配额的执行器配置。

应用停机时先停止 MQTT 接收，再等待业务队列排空，随后销毁业务 Bean。所有分区共用 5 秒期限；超过期限会丢弃未执行消息并中断正在执行的任务，业务代码需要响应中断，不能保证强制停止不响应中断的代码。

消息通过有界内存队列分发：队列满时记录拒绝错误并丢弃该次业务投递，业务异常不会自动重试。MQTT PUBACK 只表示 Broker 已接收消息；SQLite 的事务和去重作用于已进入存储流程的数据。需要持久消息保障时，可在平台接入层增加持久化、补偿和消费确认机制。

默认传输为 MQTT TCP；已有 TLS/WebSocket/证书配置时，可直接复用原 Paho Bean。SDK/Starter 不要求数据源；SQLite 仅在可选 example 中使用。
