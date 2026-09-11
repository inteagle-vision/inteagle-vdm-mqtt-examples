package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inteagle.vdm.mqtt.sdk.AlarmSnapshotPackageAssembler;
import com.inteagle.vdm.mqtt.sdk.CompletedAlarmSnapshotPackage;
import com.inteagle.vdm.mqtt.sdk.EvidencePackageChunk;
import com.inteagle.vdm.mqtt.sdk.PayloadFormat;
import com.inteagle.vdm.mqtt.sdk.RpcException;
import com.inteagle.vdm.mqtt.sdk.VdmMqttClient;
import com.inteagle.vdm.mqtt.sdk.VdmTopics;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.eclipse.paho.client.mqttv3.MqttException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Receive, verify, persist and acknowledge alarm snapshot packages. */
public final class EvidenceReceiver {
  private EvidenceReceiver() {}

  public static void main(String[] args) {
    try {
      run(args);
    } catch (Exception error) {
      System.err.println("ERROR " + error.getMessage());
      System.exit(1);
    }
  }

  private static void run(String[] args) throws Exception {
    if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h"))) {
      System.out.println("EvidenceReceiver");
      System.out.println("Set MQTT_HOST and VDM_DEVICE_ID; optional MQTT_PORT, MQTT_USERNAME, "
          + "MQTT_PASSWORD, VDM_PAYLOAD_FORMAT (protobuf or json), VDM_EVIDENCE_DIR.");
      return;
    }
    if (args.length != 0) {
      throw new IllegalArgumentException("EvidenceReceiver takes no arguments; use --help");
    }
    var config = new VdmMqttClient.Config(
        requiredEnv("MQTT_HOST"),
        Integer.parseInt(System.getenv().getOrDefault("MQTT_PORT", "1883")),
        VdmTopics.forDevice(requiredEnv("VDM_DEVICE_ID")),
        PayloadFormat.parse(System.getenv().getOrDefault("VDM_PAYLOAD_FORMAT", "protobuf")),
        System.getenv("MQTT_USERNAME"), System.getenv("MQTT_PASSWORD"), null, 1,
        Duration.ofSeconds(10), List.of("image", "rpc/resp"));
    var assembler = new AlarmSnapshotPackageAssembler(
        Path.of(System.getenv().getOrDefault("VDM_EVIDENCE_DIR", "./evidence")), 8);
    var worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(64), job -> {
          Thread thread = new Thread(job, "vdm-evidence-worker");
          thread.setDaemon(true);
          return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    var clientRef = new AtomicReference<VdmMqttClient>();
    var client = new VdmMqttClient(config, message -> {
      if (!(message.value() instanceof EvidencePackageChunk chunk)) {
        return;
      }
      try {
        // Disk verification and blocking RPC both run outside Paho's callback.
        worker.execute(() -> receive(assembler, clientRef.get(), chunk));
      } catch (RejectedExecutionException error) {
        System.err.println("RECEIVER_BUSY eventId=" + Long.toUnsignedString(chunk.eventId()));
      }
    }, error -> System.err.println("DECODE_ERROR " + error.getMessage()));
    clientRef.set(client);

    var stop = new CountDownLatch(1);
    var closed = new AtomicBoolean();
    Runnable close = () -> {
      if (closed.compareAndSet(false, true)) {
        worker.shutdownNow();
        try {
          client.close();
        } catch (Exception error) {
          System.err.println("CLOSE_ERROR " + error.getMessage());
        }
      }
      stop.countDown();
    };
    var shutdown = new Thread(close, "vdm-evidence-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdown);
    try {
      client.start();
      System.out.println("READY evidence receiver; Ctrl-C to stop");
      stop.await();
    } finally {
      close.run();
      try {
        Runtime.getRuntime().removeShutdownHook(shutdown);
      } catch (IllegalStateException ignored) {
        // JVM shutdown is already running the hook.
      }
    }
  }

  private static void receive(
      AlarmSnapshotPackageAssembler assembler, VdmMqttClient client, EvidencePackageChunk chunk) {
    CompletedAlarmSnapshotPackage completed;
    String eventId = Long.toUnsignedString(chunk.eventId());
    try {
      var result = assembler.accept(chunk);
      System.out.printf("CHUNK eventId=%s chunk=%d/%d%n",
          eventId, chunk.chunkIndex() + 1, chunk.chunkCount());
      if (result.isEmpty()) {
        return;
      }
      completed = result.get();
      System.out.println("VERIFIED eventId=" + eventId + " package=" + completed.packagePath());
    } catch (Exception error) {
      System.err.println("RECEIVE_ERROR eventId=" + eventId + " error=" + error.getMessage());
      return;
    }
    // accept() returns completion only after the TAR and receipt are durable.
    try {
      var response = withAckRetry(
          () -> client.ackEvidencePackage(
              completed.eventId(), completed.packageSha256(), Duration.ofSeconds(30)),
          Thread::sleep,
          detail -> System.err.println("ACK_RETRY eventId=" + eventId + " " + detail));
      ObjectNode fields = ((ObjectNode) response.data()).deepCopy();
      fields.put("code", 0);
      System.out.println("ACKED eventId=" + eventId
          + " packageSha256=" + completed.packageSha256() + " response=" + fields);
    } catch (Exception error) {
      System.err.println("ACK_FAILED eventId=" + eventId + " error=" + error.getMessage());
    }
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long milliseconds) throws InterruptedException;
  }

  static <T> T withAckRetry(Callable<T> operation, Sleeper sleeper, Consumer<String> retryLog)
      throws Exception {
    for (int attempt = 1; ; attempt++) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("ACK cancelled");
      }
      try {
        return operation.call();
      } catch (Exception error) {
        if (error instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw error;
        }
        if (attempt >= 4 || !isTemporaryAckFailure(error)) {
          throw error;
        }
        long delayMs = 1_000L << (attempt - 1);
        retryLog.accept("attempt=" + (attempt + 1) + "/4 delayMs=" + delayMs
            + " error=" + error.getMessage());
        try {
          sleeper.sleep(delayMs);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw interrupted;
        }
      }
    }
  }

  static boolean isTemporaryAckFailure(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof RpcException rpc) {
        return rpc.code() == 4 || rpc.code() == 5;
      }
      if (cause instanceof TimeoutException || cause instanceof java.net.SocketException
          || cause instanceof java.net.SocketTimeoutException) {
        return true;
      }
      if (cause instanceof MqttException mqtt) {
        return switch (mqtt.getReasonCode()) {
          case MqttException.REASON_CODE_BROKER_UNAVAILABLE,
              MqttException.REASON_CODE_CLIENT_TIMEOUT,
              MqttException.REASON_CODE_WRITE_TIMEOUT,
              MqttException.REASON_CODE_SERVER_CONNECT_ERROR,
              MqttException.REASON_CODE_CLIENT_NOT_CONNECTED,
              MqttException.REASON_CODE_CONNECTION_LOST,
              MqttException.REASON_CODE_NO_MESSAGE_IDS_AVAILABLE,
              MqttException.REASON_CODE_MAX_INFLIGHT,
              MqttException.REASON_CODE_DISCONNECTED_BUFFER_FULL -> true;
          default -> false;
        };
      }
    }
    return false;
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Set " + name);
    }
    return value;
  }
}
