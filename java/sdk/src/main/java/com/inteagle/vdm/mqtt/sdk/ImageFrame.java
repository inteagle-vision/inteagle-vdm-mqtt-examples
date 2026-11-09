package com.inteagle.vdm.mqtt.sdk;

/**
 * 普通图片的 Header 与 JPEG 字节。jpeg() 可直接保存为 .jpg，无需再次剥离 Header。 timestampS 是设备生成图片的 Unix 秒时间戳；sensorId
 * 标识传感器。字节数组使用防御性复制。 与告警类型 2 的 EvidencePackageChunk 不同，普通图片无需 ackEvidencePackage。
 */
public record ImageFrame(
    int version, int headerLength, int sensorId, int imageType, long timestampS, byte[] jpeg) {
  public ImageFrame {
    jpeg = jpeg.clone();
  }

  @Override
  public byte[] jpeg() {
    return jpeg.clone();
  }
}
