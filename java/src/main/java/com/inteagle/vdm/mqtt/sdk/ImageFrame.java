package com.inteagle.vdm.mqtt.sdk;

/** 已解析的 VDM 图片 Header 与 JPEG 内容。 */
public record ImageFrame(
    int version,
    int headerLength,
    int sensorId,
    int imageType,
    long timestampS,
    byte[] jpeg) {
  public ImageFrame {
    jpeg = jpeg.clone();
  }

  @Override
  public byte[] jpeg() {
    return jpeg.clone();
  }
}
