package com.inteagle.vdm.mqtt.sdk;

/** 类型 2 StdMqtt 告警证据 JPEG 分块。 */
public record EvidenceImageChunk(
    int messageType,
    int headerLength,
    int cameraId,
    int triggerType,
    long capturedAtMs,
    long eventId,
    int imageIndex,
    int imageCount,
    int actualOffsetMs,
    long jpegLength,
    byte[] jpegSha256,
    byte[] manifestSha256,
    int chunkIndex,
    int chunkCount,
    long chunkOffset,
    byte[] chunk) {
  public EvidenceImageChunk {
    jpegSha256 = jpegSha256.clone();
    manifestSha256 = manifestSha256.clone();
    chunk = chunk.clone();
  }

  @Override
  public byte[] jpegSha256() {
    return jpegSha256.clone();
  }

  @Override
  public byte[] manifestSha256() {
    return manifestSha256.clone();
  }

  @Override
  public byte[] chunk() {
    return chunk.clone();
  }
}
