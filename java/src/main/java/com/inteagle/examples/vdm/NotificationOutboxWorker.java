package com.inteagle.examples.vdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Delivers pending VDM alarm notifications to a customer HTTP webhook.
 *
 * <p>Set {@code NOTIFICATION_WEBHOOK_URL}. The receiver must persist and deduplicate the
 * {@code Idempotency-Key} header: a process failure after the remote server accepts a request but
 * before this worker records {@code SENT} can result in the request being retried.
 */
public final class NotificationOutboxWorker {
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(20);
  private static final Duration LEASE_DURATION = Duration.ofMinutes(1);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(30);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private record Options(Path database, boolean once, Duration pollInterval) {}

  private NotificationOutboxWorker() {}

  public static void main(String[] args) throws Exception {
    Options options = parseOptions(args);
    URI webhook = URI.create(requiredSetting("NOTIFICATION_WEBHOOK_URL"));
    String workerToken = "vdm-notification-worker-" + UUID.randomUUID();
    HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(HTTP_TIMEOUT)
        .build();

    try (AlarmNotificationStore store = new AlarmNotificationStore(options.database())) {
      System.out.printf("READY database=%s webhook=%s once=%s%n",
          options.database().toAbsolutePath(), webhook, options.once());
      while (true) {
        AlarmNotificationStore.Notification notification =
            store.claimNext(workerToken, LEASE_DURATION);
        if (notification == null) {
          if (options.once()) {
            return;
          }
          Thread.sleep(options.pollInterval().toMillis());
          continue;
        }

        try {
          deliver(httpClient, webhook, notification);
          store.markSent(notification);
          System.out.printf("SENT eventId=%s notification=%s%n",
              notification.eventId(), notification.notification());
        } catch (Exception error) {
          store.retryLater(notification, RETRY_DELAY, error.getMessage());
          System.err.printf("RETRY eventId=%s notification=%s delay=%ss error=%s%n",
              notification.eventId(), notification.notification(), RETRY_DELAY.toSeconds(),
              error.getMessage());
        }
        if (options.once()) {
          return;
        }
      }
    }
  }

  private static void deliver(
      HttpClient client, URI webhook, AlarmNotificationStore.Notification notification)
      throws IOException, InterruptedException {
    String body = MAPPER.writeValueAsString(Map.of(
        "deviceId", notification.deviceId(),
        "eventId", notification.eventId(),
        "alarmId", notification.alarmId(),
        "notification", notification.notification()));
    HttpRequest request = HttpRequest.newBuilder(webhook)
        .timeout(HTTP_TIMEOUT)
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", notification.idempotencyKey())
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
    HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IOException("webhook returned HTTP " + response.statusCode());
    }
  }

  private static Options parseOptions(String[] args) {
    Path database = Path.of("alarm-notifications.sqlite3");
    boolean once = false;
    Duration pollInterval = Duration.ofSeconds(2);
    for (int index = 0; index < args.length; index++) {
      switch (args[index]) {
        case "--database" -> database = Path.of(requiredArgument(args, ++index, "--database"));
        case "--once" -> once = true;
        case "--poll-seconds" -> {
          long seconds = Long.parseLong(requiredArgument(args, ++index, "--poll-seconds"));
          if (seconds < 1) {
            throw new IllegalArgumentException("--poll-seconds must be at least 1");
          }
          pollInterval = Duration.ofSeconds(seconds);
        }
        default -> throw new IllegalArgumentException("Unknown argument: " + args[index]);
      }
    }
    return new Options(database, once, pollInterval);
  }

  private static String requiredArgument(String[] args, int index, String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException(option + " requires a value");
    }
    return args[index];
  }

  private static String requiredSetting(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Set " + name);
    }
    return value;
  }
}
