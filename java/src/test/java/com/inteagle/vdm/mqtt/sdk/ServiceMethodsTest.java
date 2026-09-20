package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ServiceMethodsTest {
  @Test
  void preservesStringDeviceErrorCode() throws Exception {
    var codec = new VdmCodec(PayloadFormat.JSON);
    var p =
        codec.decode(
            "vdm/d/rpc/resp",
            VdmTopics.forDevice("d"),
            "{\"reqId\":1,\"code\":\"300\"}".getBytes());
    assertEquals(300, codec.responseInfo(p.value()).code());
  }

  @Test
  void exposesHistoryAndSyncMethodsInBothFormats() throws Exception {
    for (var format : PayloadFormat.values())
      for (var method :
          new String[] {"listAlarmEvents", "syncTelemetry", "getSyncStatus", "cancelSync"}) {
        assertNotNull(new VdmCodec(format).encodeRpcRequest(method, Map.of(), 1));
      }
  }
}
