# SDK 维护者回归测试

本目录用于验证 SDK，不随 SDK / Starter JAR 发布。业务接入见 [Java 使用说明](../../../README.md)。

在仓库根目录执行 `mvn -f java/pom.xml test`。Broker 集成测试默认跳过，使用隔离的本机 Broker 并加 `-Dvdm.test.mqtt.port=18895` 启用。
