package com.inteagle.vdm.mqtt.sdk;

/** 设备已返回响应但 code 非零；与网络断线或等待超时不同，可通过 code() 判断业务错误。 */
public final class RpcException extends RuntimeException {
  private final int reqId;
  private final int code;

  /** 保留设备响应的请求 ID 与错误码，便于日志追踪和业务分支处理。 */
  public RpcException(int reqId, int code, String message) {
    super("RPC req_id=" + reqId + " code=" + code + ": " + message);
    this.reqId = reqId;
    this.code = code;
  }

  /** 当前失败请求的 signed int32 ID。 */
  public int reqId() {
    return reqId;
  }

  /** 设备返回的业务错误码，含义以公开协议为准。 */
  public int code() {
    return code;
  }
}
