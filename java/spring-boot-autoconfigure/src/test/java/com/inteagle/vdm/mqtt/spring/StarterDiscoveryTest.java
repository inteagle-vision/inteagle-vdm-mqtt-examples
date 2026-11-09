package com.inteagle.vdm.mqtt.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** SDK 维护者回归测试：模拟应用仅引入 Starter，验证 imports 元数据自动发现配置。 */
class StarterDiscoveryTest {
  @Test
  void discoversStarterWithoutExplicitImport() {
    // 不用 AutoConfigurations.of 显式注入配置，才能发现 JAR 注册文件遗漏的问题。
    new ApplicationContextRunner()
        .withUserConfiguration(CustomerApplication.class)
        .withPropertyValues(
            "vdm.mqtt.host=localhost", "vdm.mqtt.device-id=TEST", "vdm.mqtt.auto-startup=false")
        .run(context -> assertThat(context).hasNotFailed().hasSingleBean(VdmMqttClient.class));
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  static class CustomerApplication {}
}
