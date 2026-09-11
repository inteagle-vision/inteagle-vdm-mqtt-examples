package com.inteagle.vdm.mqtt.sdk;

/** 类型 2 MQTT 告警抓拍图像 USTAR 包分块。 */
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
