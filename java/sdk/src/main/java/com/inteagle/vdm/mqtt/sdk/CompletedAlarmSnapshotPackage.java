package com.inteagle.vdm.mqtt.sdk;

import java.nio.file.Path;

/**
 * 已完整落盘并校验的抓拍包；packagePath 指向 USTAR 文件，packageSha256 可用于 ACK。 此对象只证明本地接收完成，不代表设备已收到确认；还需调用
 * ackEvidencePackage。
 */
public record CompletedAlarmSnapshotPackage(long eventId, String packageSha256, Path packagePath) {}
