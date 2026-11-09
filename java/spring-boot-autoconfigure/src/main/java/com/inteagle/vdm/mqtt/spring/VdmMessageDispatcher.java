package com.inteagle.vdm.mqtt.spring;

import com.inteagle.vdm.mqtt.sdk.DecodedPayload;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 使用 Spring 执行器分发业务消息，避免同步 RPC 等待阻塞 MQTT 响应接收。
 *
 * <p>线程创建、复用由执行器负责；本类只维护有界串行分区，使平台线程、虚拟线程及自定义执行器具有相同的设备消息顺序。
 */
final class VdmMessageDispatcher implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(VdmMessageDispatcher.class);
  private final ObjectProvider<VdmMessageListener> listeners;
  private final Executor executor;
  private final Runnable shutdownOwnedExecutor;
  private final Partition[] partitions;
  private final int queueCapacity;
  private volatile boolean closed;

  VdmMessageDispatcher(
      int concurrency, int queueCapacity, ObjectProvider<VdmMessageListener> listeners) {
    this(concurrency, queueCapacity, listeners, null, false);
  }

  VdmMessageDispatcher(
      int concurrency,
      int queueCapacity,
      ObjectProvider<VdmMessageListener> listeners,
      Executor suppliedExecutor,
      boolean virtualThreads) {
    if (concurrency < 1 || queueCapacity < 1)
      throw new IllegalArgumentException("业务分区数和队列容量必须大于 0");
    this.listeners = listeners;
    this.queueCapacity = queueCapacity;
    this.partitions = new Partition[concurrency];
    for (int i = 0; i < concurrency; i++) partitions[i] = new Partition();
    if (suppliedExecutor != null) {
      executor = suppliedExecutor;
      shutdownOwnedExecutor = () -> {}; // 应用 Bean 的生命周期由应用管理。
    } else if (virtualThreads) {
      if (Runtime.version().feature() < 21)
        throw new IllegalArgumentException(
            "VDM 虚拟线程需要 JDK 21+，JDK 17 请关闭 spring.threads.virtual.enabled");
      var tasks = new SimpleAsyncTaskExecutor("vdm-mqtt-virtual-");
      tasks.setVirtualThreads(true);
      executor = tasks;
      shutdownOwnedExecutor = tasks::close;
    } else {
      var pool = new ThreadPoolTaskExecutor();
      pool.setCorePoolSize(concurrency);
      pool.setMaxPoolSize(concurrency);
      // 每分区最多一个处理任务；短暂交接时可排队，消息积压只存于下面的有界分区队列。
      pool.setQueueCapacity(concurrency);
      pool.setThreadNamePrefix("vdm-mqtt-listener-");
      pool.setDaemon(true);
      pool.initialize();
      executor = pool;
      shutdownOwnedExecutor = pool::shutdown;
    }
  }

  /** deviceId 必须来自当前消息的 Topic，不能从单设备配置或 Payload 中猜测。 */
  void accept(String deviceId, DecodedPayload message) {
    Partition partition = partitions[Math.floorMod(deviceId.hashCode(), partitions.length)];
    Runnable delivery = () -> deliver(deviceId, message);
    synchronized (partition) {
      if (closed) throw new RejectedExecutionException("VDM 业务分发已关闭");
      if (partition.scheduled) {
        if (partition.queue.size() >= queueCapacity)
          throw new RejectedExecutionException("VDM 业务分区队列已满");
        partition.queue.addLast(delivery);
        return;
      }
      partition.scheduled = true;
      Thread submitter = Thread.currentThread();
      try {
        executor.execute(
            () -> {
              // 防止同步执行器或 CallerRunsPolicy 回退到 MQTT 线程，造成同步 RPC 死锁。
              if (Thread.currentThread() == submitter)
                throw new RejectedExecutionException(
                    "vdmMqttTaskExecutor 必须异步执行，不能使用 CallerRunsPolicy");
              drain(partition, delivery);
            });
      } catch (RuntimeException error) {
        partition.scheduled = false;
        partition.notifyAll();
        throw error;
      }
    }
  }

  private void drain(Partition partition, Runnable first) {
    synchronized (partition) {
      if (partition.cancelled) {
        partition.scheduled = false;
        partition.notifyAll();
        return;
      }
      partition.thread = Thread.currentThread();
    }
    try {
      Runnable next = first;
      while (next != null) {
        // 不持有分区锁执行业务代码，避免阻塞 MQTT 接收，也避免固定 JDK 21 虚拟线程。
        next.run();
        synchronized (partition) {
          next = partition.cancelled ? null : partition.queue.pollFirst();
          if (next == null) {
            partition.thread = null;
            partition.scheduled = false;
            partition.notifyAll();
            return;
          }
        }
      }
    } finally {
      synchronized (partition) {
        // 正常退出可能已交接给下一次提交，不能清理另一个任务的状态。
        if (partition.thread == Thread.currentThread()) {
          partition.thread = null;
          partition.scheduled = false;
          partition.queue.clear();
          partition.notifyAll();
        }
      }
    }
  }

  private void deliver(String deviceId, DecodedPayload message) {
    try {
      listeners
          .orderedStream()
          .forEach(
              listener -> {
                try {
                  listener.onMessage(deviceId, message);
                } catch (Exception error) {
                  LOG.error("VDM 业务监听器处理失败，topic=" + message.topic(), error);
                }
              });
    } catch (Exception error) {
      LOG.error("VDM 业务监听器获取失败，topic=" + message.topic(), error);
    }
  }

  /** 生命周期先停止接收，再在业务 Bean 销毁前排空；全部分区共用 5 秒期限。 */
  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    boolean interrupted = false;
    try {
      for (Partition partition : partitions) {
        synchronized (partition) {
          while (partition.scheduled && !interrupted && System.nanoTime() < deadline) {
            try {
              TimeUnit.NANOSECONDS.timedWait(partition, Math.max(1, deadline - System.nanoTime()));
            } catch (InterruptedException error) {
              interrupted = true;
            }
          }
          if (partition.scheduled) {
            LOG.warn("VDM 业务监听器关闭超时或被中断，取消未完成的内存任务");
            partition.cancelled = true;
            partition.queue.clear();
            if (partition.thread != null) partition.thread.interrupt();
          }
        }
      }
    } finally {
      shutdownOwnedExecutor.run();
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private static final class Partition {
    final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    boolean scheduled;
    boolean cancelled;
    Thread thread;
  }
}
