package com.inteagle.vdm.mqtt.sdk;

/**
 * snapshot RPC 成功回执，只表示设备已受理，不表示 JPEG 已收到。 图片随后或先于响应到达 image Topic；图片头不含 RPC reqId，不能据此承诺逐请求精确关联。
 * deviceId + sensorId 用于识别来源，同一传感器并发拍照需由业务自行协调。
 */
public record SnapshotReceipt(String deviceId, int sensorId, int requestId) {}
