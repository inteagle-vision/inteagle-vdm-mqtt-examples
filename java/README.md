# VDM 业务 SDK 使用

面向系统集成商，接收 VDM 位移、图片，查询标靶和调用设备服务。Spring Boot Starter 负责连接与订阅，业务使用 SDK 接口。默认推荐 **Protobuf**，同时支持 JSON；JDK 17+，Spring Boot 3.5.16 GA / 4.1.2。

## 按业务选择

完整代码集中在一个 [VdmBusinessExample 组件](example/src/main/java/com/inteagle/examples/boot/VdmBusinessExample.java)，按需使用：

| 业务需求 | SDK 入口 |
| --- | --- |
| 只收设备位移 | 监听 telemetry |
| 只收普通图片 | 监听 image，获得 ImageFrame |
| 接收并更新设备标靶列表（常规接法） | 订阅 attributes，使用 VdmTargets.fromAttributes(id, message) |
| 按需主动查询当前标靶（可选） | gateway.device(id).getTargets() |
| 请求设备拍照 | gateway.device(id).requestSnapshot() |

订阅范围决定接收哪些消息，不需要 profile 或不同 main。[example 说明](example/README.md) 给出首次构建、IDEA 启动和订阅配置。

## 接收位移和图片

用一个 `@Component` 实现 `VdmMessageListener`，Starter 传入来源设备 ID 和已经解码的消息。例如 Protobuf 位移：

```java
@Override
public void onMessage(String deviceId, DecodedPayload message) {
  if (message.value() instanceof Telemetry telemetry && telemetry.hasDisplacement()) {
    for (var target : telemetry.getDisplacement().getTargetsList()) {
      // 在这里处理该设备的标靶位移，单位 mm。
      System.out.printf("device=%s target=%s dx=%s%n",
          deviceId, target.getTargetId(), target.getDxList());
    }
  }
}
```

`Telemetry` 来自 SDK 内置的 `com.inteagle.vdm.mqtt.v1`。JSON 接收分支见业务组件中的 receiveDisplacement。dx/dy/dz 可以缺失；未上报 dz 时 `getDzList()` 返回空列表，不代表 0，见[方向缺失处理](example/README.md#位移方向可以缺失)。普通图片的 `message.value()` 为 `ImageFrame`，通过 `sensorId()`、`timestampS()`、`jpeg()` 获取传感器、采集时间和 JPEG 字节；两种 Payload 模式的图片接口相同。

## 标靶列表与请求图片

通常只需订阅 `attributes`，示例组件会更新该设备的标靶缓存，业务读取 `targets(deviceId)` 即可，不发送 RPC。首次接入希望立即主动查询时，才使用可选的 `getTargets()`。

注入 `VdmMqttGateway`，由业务传入要操作的设备 ID：

```java
List<VdmTarget> targets = gateway.device(deviceId).getTargets();
SnapshotReceipt receipt = gateway.device(deviceId).requestSnapshot();
```

`VdmTarget` 是统一业务模型，JSON / Protobuf 都返回相同类型；包含标靶 ID、型号、角色、传感器、ROI、距离和初始化状态。**不同设备的 targetId 可以相同，业务唯一键是 `(deviceId, targetId)`。** 接收消息的设备 ID 从实际 MQTT Topic 提取。

拍照须先订阅该设备的 `image` 和 `rpc/resp`。`SnapshotReceipt` 表示设备受理成功，图片由监听器另行接收；普通 JPEG 不携带 RPC reqId。标靶查询只需 `rpc/resp`；接收标靶后续更新再订阅 `attributes`。

## 引入 Starter

**SDK / Starter 尚未发布到 Maven Central。** 首先从本仓库安装到本机 Maven 仓库；其他电脑和 CI 也需安装，或使用团队私服。

```bash
mvn -f java/pom.xml -pl spring-boot-starter -am install
```

```xml
<dependency>
  <groupId>com.inteagle.vdm</groupId>
  <artifactId>vdm-mqtt-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

最小配置只订阅所有设备的位移：

```yaml
vdm:
  mqtt:
    host: ${MQTT_HOST}
    all-devices: true
    subscriptions: [telemetry]
```

认证通过 `vdm.mqtt.username/password` 的外部配置提供。格式默认 protobuf；JSON 设备设置 `payload-format: json`。消费 SDK 无需安装 protoc，协议类已经包含在 JAR 中。

应用项目的 `dependencyManagement` 可能覆盖 SDK 的传递依赖版本；使用已有 Spring Boot BOM 时，按[兼容工程 POM](compatibility-tests/pom.xml)中的 Jackson / Log4j 版本约束检查实际依赖，避免降回旧版。SDK 使用的 Jackson 2 与 Boot 4 默认 Jackson 3 并存，不替换应用的全局 JSON Mapper。

运行仓库源码前须由 Maven 生成 Protobuf Java 类；首次 IDEA 构建和点击启动见 [example](example/README.md#idea-首次运行)。标准通配符、动态订阅、复用已有 MQTT Bean、自定义线程池 / JDK 21 虚拟线程和其他 RPC 见[集成说明](docs/integration.md)。需要数据落库或告警时再阅读 [SQLite](docs/storage.md)、[告警](docs/alarms.md)。

## SDK 维护者回归测试

各模块 `src/test` 和 `compatibility-tests` 是 SDK 维护者回归测试，不随 SDK / Starter JAR 发布，接入 SDK 无需运行这些测试。

```bash
mvn -f java/pom.xml clean install             # 自动生成协议类、测试、检查格式并安装
mvn -f java/pom.xml spotless:apply            # 修改源码后统一代码格式
mvn -f java/compatibility-tests/pom.xml -Dspring-boot.version=4.1.2 test
```

兼容测试消费上一步安装的同一份 JAR；版本可改为 `3.5.16`。真实 MQTT 集成测试默认跳过，维护者使用隔离的本机 Broker 并加 `-Dvdm.test.mqtt.port=18895` 启用。CI 运行 JDK 17/21 与两个 Boot 版本的兼容矩阵及运行时依赖审计。
