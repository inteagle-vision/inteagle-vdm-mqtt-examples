package com.inteagle.vdm.mqtt.spring;

import com.inteagle.vdm.mqtt.paho.PahoMqttTransport;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmMqttGateway;
import com.inteagle.vdm.mqtt.sdk.VdmMqttTransport;
import java.util.concurrent.Executor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** 引入 Starter 即生效；业务自定义 VdmMqttClient Bean 时，整体让出连接管理。 */
@AutoConfiguration
@ConditionalOnClass(VdmMqttClient.class)
@ConditionalOnMissingBean({VdmMqttClient.class, VdmMqttGateway.class})
@ConditionalOnProperty(
    prefix = "vdm.mqtt",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties(VdmMqttProperties.class)
public class VdmMqttAutoConfiguration {
  private static final Log LOG = LogFactory.getLog(VdmMqttAutoConfiguration.class);

  @Bean
  VdmMessageDispatcher vdmMessageDispatcher(
      VdmMqttProperties properties,
      ObjectProvider<VdmMessageListener> listeners,
      @Qualifier("vdmMqttTaskExecutor") ObjectProvider<Executor> executors,
      Environment environment) {
    properties.clientConfig(); // 先校验，再分配后台资源。
    return new VdmMessageDispatcher(
        properties.getListenerConcurrency(),
        properties.getListenerQueueCapacity(),
        listeners,
        executors.getIfAvailable(),
        environment.getProperty("spring.threads.virtual.enabled", Boolean.class, false));
  }

  @Bean(destroyMethod = "close")
  VdmMqttGateway vdmMqttGateway(
      VdmMqttProperties properties, VdmMessageDispatcher dispatcher, VdmMqttTransport transport)
      throws Exception {
    if (properties.getTopicFilters() != null) {
      return new VdmMqttGateway(
          properties.clientConfig(),
          properties.getTopicFilters(),
          transport,
          dispatcher::accept,
          error -> LOG.error("VDM MQTT 接收、解码或投递失败", error));
    }
    return new VdmMqttGateway(
        properties.clientConfig(),
        properties.selectedDeviceIds(),
        properties.isAllDevices(),
        transport,
        dispatcher::accept,
        error -> LOG.error("VDM MQTT 接收、解码或投递失败", error));
  }

  /** 单设备配置保持最简单的注入方式；多设备通过 gateway.device(id) 选择逻辑客户端。 */
  @Bean(destroyMethod = "")
  @Conditional(SingleDeviceConfigured.class)
  VdmMqttClient vdmMqttClient(VdmMqttGateway gateway, VdmMqttProperties properties) {
    return gateway.device(properties.getDeviceId());
  }

  static class SingleDeviceConfigured implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return org.springframework.util.StringUtils.hasText(
          context.getEnvironment().getProperty("vdm.mqtt.device-id"));
    }
  }

  /** 用户提供 VdmMqttTransport Bean 时不创建 Paho；排除 Paho 依赖时也可运行核心配置。 */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(PahoMqttTransport.class)
  static class PahoConfiguration {
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean(VdmMqttTransport.class)
    VdmMqttTransport vdmMqttTransport(VdmMqttProperties properties) throws Exception {
      // 连接的唯一关闭责任归 VdmMqttGateway，防止共享资源被重复释放。
      if (properties.getHost() == null || properties.getHost().isBlank()) {
        throw new IllegalArgumentException("默认 MQTT 连接必须配置 vdm.mqtt.host");
      }
      return new PahoMqttTransport(properties.clientConfig());
    }
  }

  @Bean
  VdmMqttLifecycle vdmMqttLifecycle(
      VdmMqttGateway client, VdmMqttProperties properties, VdmMessageDispatcher dispatcher) {
    return new VdmMqttLifecycle(client, properties.isAutoStartup(), dispatcher);
  }
}
