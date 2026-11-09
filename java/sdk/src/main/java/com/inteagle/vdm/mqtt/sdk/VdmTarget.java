package com.inteagle.vdm.mqtt.sdk;

/**
 * 与传输格式无关的公开标靶模型。唯一业务键是 deviceId + targetId，targetId 不是全局 ID。 distanceMeters 单位 m；roi 可为
 * null（例如虚拟标靶 @DEVICE）；初始化时间为 Unix 秒。
 */
public record VdmTarget(
    String deviceId,
    String targetId,
    int sensorId,
    Roi roi,
    double distanceMeters,
    Role role,
    Model model,
    boolean skipMeasurement,
    boolean initialized,
    Long initializedAtSeconds) {
  public enum Role {
    UNSPECIFIED,
    MP,
    RP,
    CP
  }

  public enum Model {
    UNSPECIFIED,
    T10,
    T20,
    T50,
    T100,
    T200
  }

  /** 原始相机图像上的像素矩形。 */
  public record Roi(long x, long y, long width, long height) {}
}
