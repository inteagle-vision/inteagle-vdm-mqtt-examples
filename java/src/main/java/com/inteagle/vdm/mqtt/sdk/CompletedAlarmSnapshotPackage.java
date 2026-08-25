package com.inteagle.vdm.mqtt.sdk;

import java.nio.file.Path;

/** 已完整持久化并通过校验的告警抓拍图像包。 */
public record CompletedAlarmSnapshotPackage(
    long eventId,
    String packageSha256,
    Path packagePath) {}
