#!/usr/bin/env bash
set -euo pipefail
JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 先走 Maven 生命周期，保证全新 checkout 也会生成协议、编译并打包所有依赖模块。
mvn -B -ntp -f "${JAVA_DIR}/pom.xml" -pl example -am -DskipTests package
exec java -jar "${JAVA_DIR}/example/target/vdm-mqtt-example-1.0.0-boot.jar" "$@"
