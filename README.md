# Inteagle VDM MQTT 接入示例

用于系统集成商的设备接入：连接设备使用的 MQTT Broker，接收位移与图片，按需查询设备或使用告警能力。**推荐 Protobuf，所有 Java 最小示例默认使用 Protobuf；JSON 作为兼容选项。**

先选择开发语言，再按业务需求配置订阅。

| 语言 | 接入入口 |
| --- | --- |
| Java / Spring Boot | [Starter、位移、图片、标靶查询](java/README.md) |
| Python | [安装与接收数据](python/README.md) |
| Go | [安装与接收数据](go/README.md) |
| JavaScript / TypeScript | [安装与接收数据](javascript/README.md) |

Java 提供独立 SDK 与 Spring Boot Starter，支持 Spring Boot 3.5.16 GA / 4.1.2、JDK 17 / 21；IDEA 启动步骤见 [Java 示例](java/example/README.md)。

公共参考：[Broker 与 Topic / Schema](docs/protocol.md) · [Docker 联调](docs/docker.md) · [告警业务规则](examples/alarms/README.md)。

Java SDK 和 Starter **尚未发布到 Maven Central**，先完成 [Maven 本地安装](java/README.md)。各模块的回归测试用于 SDK 维护。

许可证：[Apache-2.0](LICENSE)。
