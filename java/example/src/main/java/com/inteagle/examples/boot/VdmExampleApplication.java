package com.inteagle.examples.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 唯一 Spring Boot 启动入口；订阅列表选择要接收的数据，RPC 由业务服务显式调用。 */
@SpringBootApplication
public class VdmExampleApplication {
  public static void main(String[] args) {
    SpringApplication.run(VdmExampleApplication.class, args);
  }
}
