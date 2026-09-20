package com.inteagle.vdm.service;

import com.inteagle.vdm.service.alarms.AlarmService;
import com.inteagle.vdm.service.evidence.EvidenceService;
import java.nio.file.*;
import java.util.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ServiceApplication {
  public static void main(String[] args) throws Exception {
    String host = System.getenv().getOrDefault("VDM_HTTP_HOST", "127.0.0.1");
    ServiceConfig.validateBind(host, System.getenv("VDM_API_TOKEN"));
    var app = new SpringApplication(ServiceApplication.class);
    app.setDefaultProperties(
        Map.of(
            "server.address",
            host,
            "server.port",
            System.getenv().getOrDefault("VDM_HTTP_PORT", "8080"),
            "server.tomcat.threads.max",
            64,
            "server.tomcat.max-connections",
            256,
            "server.error.include-message",
            "never"));
    app.run(args);
  }

  @Bean
  org.springframework.boot.web.server.WebServerFactoryCustomizer<
          org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory>
      bindPolicy() {
    return factory -> {
      try {
        String host = System.getenv().getOrDefault("VDM_HTTP_HOST", "127.0.0.1");
        ServiceConfig.validateBind(host, System.getenv("VDM_API_TOKEN"));
        factory.setAddress(java.net.InetAddress.getByName(host));
        int port = Integer.parseInt(System.getenv().getOrDefault("VDM_HTTP_PORT", "8080"));
        if (port < 1 || port > 65535) throw new IllegalArgumentException("invalid HTTP port");
        factory.setPort(port);
      } catch (Exception error) {
        throw new IllegalArgumentException("invalid HTTP binding", error);
      }
    };
  }

  @Bean
  ServiceConfig serviceConfig() throws Exception {
    return ServiceConfig.load(
        Path.of(
            System.getenv()
                .getOrDefault("VDM_SERVICE_CONFIG", "../contracts/config.example.json")));
  }

  @Bean(destroyMethod = "close")
  Store store(ServiceConfig config) throws Exception {
    return new Store(
        data(),
        config.maxInboxRows(),
        config.notificationMaxAgeSeconds(),
        config.maxEvidenceBytes());
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  DeviceRegistry registry(ServiceConfig config, Store store) throws Exception {
    return new DeviceRegistry(config, store, data());
  }

  @Bean
  Operations operations() throws Exception {
    return new Operations();
  }

  @Bean
  RpcGateway rpc(DeviceRegistry registry, ServiceConfig config, Store store) {
    return new RpcGateway(registry, config, store);
  }

  @Bean
  AlarmService alarms(Store store, RpcGateway rpc) {
    return new AlarmService(store, rpc);
  }

  @Bean
  EvidenceService evidence(Store store, ServiceConfig config) throws Exception {
    return new EvidenceService(store, data().resolve("evidence"), config.maxEvidenceBytes());
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  Workers workers(
      Store store,
      DeviceRegistry registry,
      ServiceConfig config,
      RpcGateway rpc,
      EvidenceService evidence,
      AlarmService alarms) {
    return new Workers(
        store,
        registry,
        config,
        rpc,
        evidence,
        alarms,
        System.getenv("VDM_WEBHOOK_URL"),
        System.getenv("VDM_WEBHOOK_TOKEN"));
  }

  static Path data() {
    return Path.of(System.getenv().getOrDefault("VDM_DATA_DIR", "./data"));
  }
}
