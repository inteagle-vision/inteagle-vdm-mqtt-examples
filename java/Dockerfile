# syntax=docker/dockerfile:1.7

FROM maven:3.9.9-eclipse-temurin-21@sha256:3a4ab3276a087bf276f79cae96b1af04f53731bec53fb2e651aca79e4b10211e AS builder

WORKDIR /src
COPY java /src/java
COPY proto /src/proto
COPY examples /src/examples
COPY LICENSE /src/LICENSE
WORKDIR /src/java
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress -pl example -am package

FROM eclipse-temurin:21-jre@sha256:10c64ccda26cc3505fcf09b4b766ae7a530492e63bf5ec89b7217aa421d18a85
WORKDIR /app
COPY --from=builder /src/java/example/target/vdm-mqtt-example-1.0.0-cli.jar /app/vdm-mqtt-consumer.jar
ENTRYPOINT ["java", "-jar", "/app/vdm-mqtt-consumer.jar"]
