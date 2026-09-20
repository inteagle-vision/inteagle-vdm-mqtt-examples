# 依赖与构建基线

固定现有工具链，新增 Web/SQLite 依赖按实际安装与构建结果核验，不按“最新”猜版本。

| 语言 | 服务依赖 | 锁定来源 | 验证环境 |
|---|---|---|---|
| Python | FastAPI 0.115.12, uvicorn 0.34.2, Paho 2.1.0, Protobuf 5.29.5 | requirements.lock 全依赖锁 | Python 3.11.7 / Docker 3.12 |
| Go | Paho 1.5.0, modernc/sqlite 1.34.5, Protobuf 1.36.5 | go.mod / go.sum | Go 1.23.4 / Docker 1.24 |
| Java | Spring Boot 3.4.13, Paho 1.2.5, sqlite-jdbc 3.49.1.0 | 固定父 BOM 与 pom.xml 版本 | JDK 21 / Maven 3.9 |
| JavaScript | Express 5.2.1, better-sqlite3 11.10.0, MQTT.js 5.15.2 | package-lock.json | Node 20.19.4 / Docker 22 |

兼容性依据：[FastAPI 版本说明](https://fastapi.tiangolo.com/deployment/versions/)、
[Spring Boot 3.4 系统要求](https://docs.spring.io/spring-boot/3.4/system-requirements.html)、
[Express 5 迁移要求](https://expressjs.com/en/guide/migrating-5/)、
[modernc SQLite](https://pkg.go.dev/modernc.org/sqlite@v1.34.5)。
Docker 基础镜像沿用仓库已锁定的 digest。CI 不依赖真实设备或固定测试端口。
