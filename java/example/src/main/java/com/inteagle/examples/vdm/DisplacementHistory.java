package com.inteagle.examples.vdm;

import java.nio.file.Path;

/** 离线查询本地时序数据的入口；不连接 MQTT，不修改设备状态。 */
public final class DisplacementHistory {
  private DisplacementHistory() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      throw new IllegalArgumentException(
          "用法: DisplacementHistory 数据库路径 设备ID 标靶ID 起始Unix毫秒 结束Unix毫秒");
    }
    // 与接收器使用同一个 SQLite 文件；WAL 允许接收期间查询，默认最多显示 1000 点。
    try (var store = new DisplacementStore(Path.of(args[0]))) {
      for (var sample :
          store.query(args[1], args[2], Long.parseLong(args[3]), Long.parseLong(args[4]), 1000)) {
        System.out.println(sample);
      }
    }
  }
}
