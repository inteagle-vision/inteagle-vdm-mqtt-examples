# 运行业务 SDK 示例

一个启动类 [VdmExampleApplication](src/main/java/com/inteagle/examples/boot/VdmExampleApplication.java)，一个业务组件 [VdmBusinessExample](src/main/java/com/inteagle/examples/boot/VdmBusinessExample.java)。位移、图片、标靶和 RPC 的使用说明直接写在组件注释中。

## IDEA 首次运行

1. 打开仓库根目录，将 `java/pom.xml` 添加为 Maven 项目，使用 JDK 17+。
2. **先生成并编译 Protobuf 类**：按终端当前目录选择一条命令，然后在 IDEA 点击 **Reload All Maven Projects**。

| 终端当前目录 | 命令 |
| --- | --- |
| 仓库根目录（包含 `java/` 和 `proto/`） | `mvn -f java/pom.xml -pl example -am compile` |
| 已进入 `java/` 目录 | `mvn -pl example -am compile` |

若已经执行过完整的 `clean install`，无需再单独执行 `compile`。`java/java/pom.xml` 不存在表示当前已在 `java/`，应使用第二行命令。

生成位置是 `java/sdk/target/generated-sources/protobuf/java`，Maven 已显式注册为源码目录。`com.inteagle.vdm.mqtt.v1` 不存在时先检查这一步；首次检出、`mvn clean` 后，IDEA 自身的 Java 编译不会自动执行 protoc。

3. 配置 Broker 地址、端口及认证信息。可以直接使用 `application.yml` 中的 `vdm.mqtt.*` 配置，或在 IDEA 运行配置的环境变量中填写 `MQTT_HOST`、`MQTT_PORT`、`MQTT_USERNAME`、`MQTT_PASSWORD`。下方的本地 properties 文件只是另一种可选方式，不提交真实凭据。
4. 选择共享运行配置 **VDM Example** 点击 Run / Debug，它带有 Maven 前置编译任务。使用主类绿色按钮创建临时配置时，在 IDEA `Maven → Runner` 启用 `Delegate IDE build/run actions to Maven`，或添加同样的 Maven 前置任务。

应用持续运行，点击 Stop 退出。命令行也可运行 `./java/run-spring-boot.sh`，脚本自动完成构建和协议生成。

JDK 21+ 可在本地配置中增加 `spring.threads.virtual.enabled=true` 启用虚拟线程。JDK 17 使用默认线程池即可；两种方式都保持同设备消息顺序。

## 配置文件可以任选一种

`application.yml` 和 `.properties` 中的 `vdm.mqtt.*` 是同一组 Starter 配置，只是语法不同。示例中的 `spring.config.import: optional:file:./vdm-example-local.properties` 仅用于加载可选的本地配置：文件不存在时会跳过；不用这个文件时可以删除该 import，通过 YAML、环境变量或启动参数提供配置。`optional` 不会免除 MQTT 地址和认证的配置要求。

如果使用 [本地配置模板](vdm-example-local.properties.example)，将其复制为运行工作目录下的 `vdm-example-local.properties`。共享运行配置 **VDM Example** 的工作目录是仓库根目录；若在 `java/` 目录运行，则文件放在 `java/vdm-example-local.properties`。这里的 `./` 指进程工作目录，并非 `application.yml` 所在目录。

导入文件里的同名配置覆盖声明 import 的 YAML；通常选一个地方维护同一配置即可。规则见 [Spring Boot 外部配置说明](https://docs.spring.io/spring-boot/3.5/reference/features/external-config.html)。

## 只配置需要的订阅

**不填写 `vdm.mqtt.subscriptions` 时，Starter 默认订阅 `[telemetry]`，其中包含位移数据。** 它填写的是 Topic 后缀，不是设备 ID 或完整 Topic。例如下面两种写法等价，任选一种：

```yaml
vdm:
  mqtt:
    subscriptions: [telemetry, image, attributes]
```

```properties
vdm.mqtt.subscriptions=telemetry,image,attributes
```

公共配置只有一份 `application.yml`，不需要 profile 或另一个 main。按业务选择订阅列表：

| 业务需求 | vdm.mqtt.subscriptions |
| --- | --- |
| 只收位移（默认） | telemetry |
| 只收普通图片 | image |
| 同时接收位移、图片和标靶更新 | telemetry,image,attributes |
| 接收标靶更新 | attributes |
| 主动查询标靶并接收更新 | attributes,rpc/resp |
| 请求拍照并接收图片 | image,rpc/resp |

显式配置列表会替换默认值：只填 `image` 就不再订阅 telemetry。示例使用 `all-devices: true`，上述三项实际订阅 `vdm/+/telemetry`、`vdm/+/image`、`vdm/+/attributes`。需要不同设备订阅不同类型时使用完整的 `topic-filters`；该模式不会追加默认 telemetry。支持的全部后缀和动态订阅见[按需订阅](../docs/integration.md#按需订阅)。

组件根据收到的 Topic 调用对应接收分支；未订阅的业务不会收到数据。默认推荐 Protobuf，JSON 设备设置 `vdm.mqtt.payload-format=json`，或使用示例的环境变量 `VDM_PAYLOAD_FORMAT=json`。修改配置文件后需重启应用。

标靶列表通常只需订阅 `attributes`，组件自动更新缓存；业务调用 `targets(deviceId)` 读取，不必再发查询 RPC。需要立即主动查询当前列表时才使用 `queryTargets(deviceId)`。

主动查询/拍照由业务调用组件内的 `queryTargets(deviceId)` / `requestImage(deviceId, sensorId)` 示例方法；它们直接使用 Starter 注入的 `VdmMqttGateway`。**启动时不自动发送 RPC**，不需要额外 RPC 组件。不同设备的同名标靶按 `(deviceId, targetId)` 区分。

调用时 SDK 向该设备的 `vdm/{deviceId}/rpc/req` **发布**请求；平台只需订阅 `rpc/resp` 接收响应，不必把 `rpc/req` 加入 subscriptions。Broker 账号须具有相应的发布和订阅权限。

## 示例入口与回归测试

`com.inteagle.examples.boot` 是 Spring Boot 接入示例，日常运行 `VdmExampleApplication` 即可。`com.inteagle.examples.vdm` 中的 `ReceiveData`、`AlarmRpc` 等是独立 CLI 示例，通过各自的命令参数或环境变量配置，不加载 Spring 的 `application.yml`；该包还包含 Boot 示例复用的 SQLite 存储类。

`src/test` 是 SDK 维护者回归测试，与上述可运行示例分开，无需作为接入步骤执行。

## 位移方向可以缺失

`getDxList/getDyList/getDzList` 在相应方向未上报时返回非 null 的空列表，对应 Count 为 0；空数组不等于 `[0.0]`。读取 `getDz(i)` 前检查 `getDzCount()`，缺少 dz 时继续处理已有的 dx/dy，不补零。设备也可能只上报 dz。

JSON 缺少 dz 或 `dz: []` 同样表示没有该方向的数据。未上报可能是型号不支持、未配置或本批无数据，不能据此认定永久不支持。各非空方向使用同一采样时间网格。

需要保存位移历史时设置 `demo.storage.enabled=true`，示例 SQLite 缺失方向写 NULL，真实零值写 0；详见[存储说明](../docs/storage.md)。[业务 SDK 首页](../README.md)介绍 SDK 接口，[集成说明](../docs/integration.md)介绍动态订阅和复用 MQTT 客户端。
