# Java 发布

发布模块：parent、sdk、mqtt-paho、spring-boot-autoconfigure、spring-boot-starter。example 与 compatibility-tests 不发布到 Maven Central。

使用 JDK 17 构建，先通过 `.github/workflows/smoke.yml` 的格式、协议、MQTT 集成和 Spring Boot 兼容检查。许可证为仓库根目录的 Apache-2.0。

## 本地发布包

```bash
mvn -f java/pom.xml -pl spring-boot-starter -am -Prelease clean deploy \
  -Dgpg.keyname=YOUR_SIGNING_KEY_FINGERPRINT
```

`release` 是 Maven 构建配置，不是应用运行 profile。它生成源码包、Javadoc、GPG 签名和校验值；默认 `central.skipUpload=true`，仅生成本地 bundle，不上传。

GPG 口令通过本机代理或交互输入；Central 用户令牌放在本机 `~/.m2/settings.xml` 的 `central` server 中。私钥、口令和令牌不写入仓库。只验证附件和打包结构时可以加 `-Dgpg.skip=true`，但生成的未签名 bundle 不能发布。

Starter 仅聚合依赖，没有独立 Java API；其 Javadoc 附件说明 API 所在模块，具体 API 文档与源码随 SDK、Paho 和自动配置模块发布。

## 上传与发布

确认签名、公钥可查询、命名空间权限和 Central 账号资格后，将同一命令增加 `-Dcentral.skipUpload=false` 上传。`autoPublish=false` 保持人工发布：先在 Central Portal 查看验证结果，再发布该版本。

正式发布前固定源码提交和版本；Central 已发布版本不能覆盖。确认制品可以从 Central 下载后，才更新 README 中的安装说明并创建对应 GitHub Release。

要求与操作以 [Central 发布文档](https://central.sonatype.org/publish/publish-portal-maven/)及[账号发布资格](https://central.sonatype.org/publish/maven-central-publishing-limits/)为准。
