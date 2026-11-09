package com.inteagle.vdm.mqtt.sdk;

/**
 * 类型 2 告警抓拍 USTAR 包的一块，不是可直接保存为 JPEG 的图片。 eventId 是 uint64 的 long 位模式，展示时使用
 * Long.toUnsignedString；chunkIndex 从 0 开始。 packageSha256 是整个包的 32 字节摘要，chunkOffset 是块在包中的字节偏移。 交给
 * AlarmSnapshotPackageAssembler 重组并校验，再执行应用层 ACK。
 */
public record EvidencePackageChunk(
    int messageType,
    int headerLength,
    int packageFormat,
    int evidenceKind,
    long eventId,
    long packageLength,
    byte[] packageSha256,
    long chunkIndex,
    long chunkCount,
    long chunkOffset,
    byte[] chunk) {
  public EvidencePackageChunk {
    packageSha256 = packageSha256.clone();
    chunk = chunk.clone();
  }

  @Override
  public byte[] packageSha256() {
    return packageSha256.clone();
  }

  @Override
  public byte[] chunk() {
    return chunk.clone();
  }
}
