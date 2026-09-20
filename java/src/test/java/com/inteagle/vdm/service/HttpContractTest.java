package com.inteagle.vdm.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.inteagle.vdm.service.device.DeviceController;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class HttpContractTest {
  @TempDir Path dir;
  Store store;
  DeviceRegistry registry;
  MockMvc mvc;

  @BeforeEach
  void setup() throws Exception {
    var config =
        new ServiceConfig(
            List.of(
                new ServiceConfig.Broker(
                    "one",
                    "127.0.0.1",
                    1883,
                    null,
                    null,
                    List.of(new ServiceConfig.Device("DEMO", "json", Set.of())))),
            100,
            300,
            100,
            7,
            1048576);
    store = new Store(dir, 100, 300);
    registry = new DeviceRegistry(config, store, dir);
    var gateway = new RpcGateway(registry, config, store);
    mvc =
        MockMvcBuilders.standaloneSetup(
                new DeviceController(new Operations(), gateway),
                new com.inteagle.vdm.service.evidence.EvidenceController(new Operations(), gateway))
            .setControllerAdvice(new HttpErrors())
            .addFilters(new HttpSecurity())
            .build();
  }

  @AfterEach
  void close() throws Exception {
    registry.close();
    store.close();
  }

  @Test
  void rejectsTrailingJsonBeforePublishingRpc() throws Exception {
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/reboot")
                .contentType("application/json")
                .content("{}{}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value("INVALID_ARGUMENT"));
  }

  @Test
  void rejectsUnknownBodyAndUnknownDeviceBeforeNetwork() throws Exception {
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/reboot")
                .contentType("application/json")
                .content("{\"other\":1}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error.code").value("INVALID_ARGUMENT"));
    mvc.perform(
            post("/v1/connections/one/devices/nope/device/reboot")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isNotFound());
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/arbitrary")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isNotFound());
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/reboot")
                .contentType("application/json")
                .content("[]"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void doesNotAcceptAsyncWithoutDeviceSuccess() throws Exception {
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/reboot")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.error.code").value("UNAVAILABLE"));
  }

  @Test
  void rejectsUnverifiedHardwareAndOversizedBody() throws Exception {
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/motor/angle/update")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isConflict());
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/device/reboot")
                .contentType("application/json")
                .content(" ".repeat(1048577)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void rejectsEvidenceAckWithoutVerifiedReceipt() throws Exception {
    mvc.perform(
            post("/v1/connections/one/devices/DEMO/evidence/ack")
                .contentType("application/json")
                .content(
                    "{\"params\":{\"eventId\":\"9\",\"packageSha256\":\""
                        + "a".repeat(64)
                        + "\",\"kind\":\"SNAPSHOT\"}}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void remoteBindRequiresSecretAndOperationMapContains33() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> ServiceConfig.validateBind("0.0.0.0", null));
    ServiceConfig.validateBind("127.0.0.1", null);
    assertEquals(33, new Operations().all().size());
  }
}
