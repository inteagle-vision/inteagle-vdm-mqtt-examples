package com.inteagle.vdm.mqtt.sdk;

/** 设备返回非零 RPC code。 */
public final class RpcException extends RuntimeException {
  private final int reqId;
  private final int code;

  public RpcException(int reqId, int code, String message) {
    super("RPC req_id=" + reqId + " code=" + code + ": " + message);
    this.reqId = reqId;
    this.code = code;
  }

  public int reqId() {
    return reqId;
  }

  public int code() {
    return code;
  }
}
