package com.inteagle.examples.boot;

import com.inteagle.examples.vdm.DisplacementStore;
import com.inteagle.vdm.mqtt.sdk.DecodedPayload;
import com.inteagle.vdm.mqtt.sdk.ImageFrame;
import com.inteagle.vdm.mqtt.sdk.SnapshotReceipt;
import com.inteagle.vdm.mqtt.sdk.VdmMqttGateway;
import com.inteagle.vdm.mqtt.sdk.VdmTarget;
import com.inteagle.vdm.mqtt.sdk.VdmTargets;
import com.inteagle.vdm.mqtt.spring.VdmMessageListener;
import com.inteagle.vdm.mqtt.v1.Telemetry;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 系统集成商的业务接入示例：一个组件展示接收位移/图片、更新标靶以及调用设备 RPC。
 *
 * <p>application.yml 的 subscriptions 决定接收哪些数据；只订阅 telemetry 时其余接收分支不会执行。 RPC
 * 由业务显式调用，本组件不会在启动时自动查询或拍照。连接、协议解析和请求关联由 Starter/SDK 完成。
 */
@Component
public class VdmBusinessExample implements VdmMessageListener {
  private static final Logger LOG = LoggerFactory.getLogger(VdmBusinessExample.class);
  private final VdmMqttGateway gateway;
  private final Path imageDirectory;
  private final DisplacementStore store;
  // 不同设备下可以有同名标靶 T01。业务唯一键是 (deviceId, targetId)，不能只用 targetId。
  // Starter 可并行处理不同设备，共享缓存需要线程安全；每台设备的列表保持不可变。
  private final Map<String, List<VdmTarget>> targetsByDevice = new ConcurrentHashMap<>();

  public VdmBusinessExample(
      VdmMqttGateway gateway,
      @Value("${demo.image-directory:${VDM_IMAGE_DIR:./images}}") Path imageDirectory,
      @Value("${demo.storage.enabled:false}") boolean saveDisplacement,
      @Value("${demo.database:${VDM_SQLITE_PATH:./vdm.sqlite3}}") Path database)
      throws Exception {
    this.gateway = gateway;
    this.imageDirectory = imageDirectory;
    // SQLite 只是可选业务示例；默认不建库。集成商可在接收分支中替换为自己的数据服务。
    this.store = saveDisplacement ? new DisplacementStore(database) : null;
  }

  @Override
  public void onMessage(String deviceId, DecodedPayload message) throws Exception {
    // deviceId 来自实际 vdm/{deviceId}/... Topic，不能用 Payload 自报设备 ID 替代。
    switch (message.suffix()) {
      case "telemetry" -> receiveDisplacement(deviceId, message);
      case "image" -> receiveImage(deviceId, message);
      // 标靶列表的常规获取方式：订阅 attributes 后直接更新缓存，不需要额外发送 getTargets RPC。
      case "attributes" ->
          VdmTargets.fromAttributes(deviceId, message)
              .ifPresent(targets -> replaceTargets(deviceId, targets));
      default -> {
        /* rpc/resp 已由 SDK 关联请求；event、3A 可按业务需求增加处理。 */
      }
    }
  }

  private void receiveDisplacement(String deviceId, DecodedPayload message) throws Exception {
    if (message.value() instanceof Telemetry telemetry && telemetry.hasDisplacement()) {
      for (var target : telemetry.getDisplacement().getTargetsList()) {
        // dx/dy/dz 都是批次样本，单位 mm。某方向未上报时 getXxList() 是非 null 空列表，不是数值 0。
        // getDz(i) 前检查 getDzCount()；dz 为空照常处理 dx/dy，只有 dz 的设备也合法。
        LOG.info(
            "DISPLACEMENT device={} target={} dx={} dy={} dz={}",
            deviceId,
            target.getTargetId(),
            target.getDxList(),
            target.getDyList(),
            target.getDzList());
      }
    } else if (message.data().has("disp")) {
      // JSON 位移位于 disp.d；缺少 dz 或 dz:[] 同样表示该方向没有数据，不能补零。
      LOG.info("DISPLACEMENT device={} data={}", deviceId, message.data().path("disp").path("d"));
    }
    if (store != null) store.save(deviceId, message); // 缺失方向写 NULL，真实零位移写 0。
  }

  private void receiveImage(String deviceId, DecodedPayload message) throws Exception {
    // 普通 JPEG 与告警证据包共用 image Topic；此处只保存普通图片。
    if (!(message.value() instanceof ImageFrame image)) return;
    Files.createDirectories(imageDirectory);
    String device = java.util.HexFormat.of().formatHex(deviceId.getBytes(StandardCharsets.UTF_8));
    // 设备 ID 编码为安全文件名前缀，临时文件后缀避免同一秒的图片互相覆盖。
    Path output =
        Files.createTempFile(
            imageDirectory,
            device + "-sensor-" + image.sensorId() + "-" + image.timestampS() + "-",
            ".jpg");
    Files.write(output, image.jpeg());
    LOG.info("IMAGE device={} sensor={} file={}", deviceId, image.sensorId(), output);
  }

  /**
   * 可选的主动查询：首次接入需要立即获取当前列表时使用，须先订阅该设备的 rpc/resp。
   *
   * <p>常规接入只需订阅 attributes；收到列表后直接用 targets(deviceId) 读取，不必调用本方法。 JSON/Protobuf 都返回
   * VdmTarget；离线/超时向业务抛异常，不将失败当成空列表。
   */
  public List<VdmTarget> queryTargets(String deviceId) throws Exception {
    List<VdmTarget> targets = gateway.device(deviceId).getTargets();
    replaceTargets(deviceId, targets);
    return targets;
  }

  /**
   * 业务主动请求拍照，须先订阅该设备的 image 和 rpc/resp。 回执仅表示受理成功；JPEG 通过 receiveImage 另行接收，没有 RPC
   * reqId，不保证并发请求与图片一一对应。
   */
  public SnapshotReceipt requestImage(String deviceId, int sensorId) throws Exception {
    return gateway.device(deviceId).requestSnapshot(sensorId, Duration.ofSeconds(10));
  }

  private void replaceTargets(String deviceId, List<VdmTarget> targets) {
    // attributes 未携带列表时不会调用这里；明确空列表才清空该设备，其他设备不受影响。
    targetsByDevice.put(deviceId, List.copyOf(targets));
    for (VdmTarget target : targets) {
      LOG.info(
          "TARGET device={} target={} sensor={} model={} role={} initialized={}",
          deviceId,
          target.targetId(),
          target.sensorId(),
          target.model(),
          target.role(),
          target.initialized());
    }
  }

  /** 读取 attributes 更新的设备标靶缓存，不发送 RPC；平台可替换为自己的数据库查询。 */
  public List<VdmTarget> targets(String deviceId) {
    return targetsByDevice.getOrDefault(deviceId, List.of());
  }

  @PreDestroy
  public void close() throws Exception {
    if (store != null) store.close();
  }
}
