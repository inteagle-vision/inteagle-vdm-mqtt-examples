#!/usr/bin/env bash
set -euo pipefail
JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE="${1:-ReceiveData}"
if [[ $# -gt 0 ]]; then shift; fi
case "${EXAMPLE}" in
  DisplacementHistory|ReceiveData|AlarmRpc|EvidenceReceiver|AlarmNotificationConsumer|NotificationOutboxWorker|Consumer) ;;
  *) echo "未知 Java 示例: ${EXAMPLE}" >&2; exit 2 ;;
esac
mvn -B -ntp -f "${JAVA_DIR}/pom.xml" -pl example -am -DskipTests package
exec java -cp "${JAVA_DIR}/example/target/vdm-mqtt-example-1.0.0-cli.jar" "com.inteagle.examples.vdm.${EXAMPLE}" "$@"
