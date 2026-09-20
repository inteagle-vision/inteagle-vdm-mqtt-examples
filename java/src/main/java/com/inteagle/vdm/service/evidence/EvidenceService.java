package com.inteagle.vdm.service.evidence;

import com.inteagle.vdm.mqtt.sdk.*;
import com.inteagle.vdm.service.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/** Replayable inbox chunks -> SDK verification -> receipt -> persistent application ACK job. */
public final class EvidenceService {
  private final Store store;
  private final Path root;
  private final long maximum;
  private final Map<String, AlarmSnapshotPackageAssembler> assemblers = new HashMap<>();

  public EvidenceService(Store store, Path root, long maximum) throws Exception {
    this.store = store;
    this.root = root;
    this.maximum = maximum;
    Files.createDirectories(root);
  }

  public synchronized void accept(long inboxId, String c, String d, Object value) throws Exception {
    synchronized (store) {
      acceptStored(inboxId, c, d, value);
    }
  }

  private void acceptStored(long inboxId, String c, String d, Object value) throws Exception {
    Path device = root.resolve(c).resolve(d);
    Files.createDirectories(device);
    if (value instanceof ImageFrame image) {
      Path target = device.resolve("image-" + inboxId + ".jpg");
      if (!Files.exists(target)) {
        capacity(image.jpeg().length, inboxId);
        write(target, image.jpeg());
      }
      store.update("update inbox set status='done' where id=?", inboxId);
      return;
    }
    var chunk = (EvidencePackageChunk) value;
    String sha = HexFormat.of().formatHex(chunk.packageSha256());
    String key = c + "/" + d + "/" + Long.toUnsignedString(chunk.eventId()) + "/" + sha;
    Path dir = device.resolve(Long.toUnsignedString(chunk.eventId()));
    Path partial = dir.resolve(sha + ".tar.part"), complete = dir.resolve(sha + ".tar");
    // Reserve the entire package at its first chunk, including receipt overhead.
    if (!Files.exists(partial) && !Files.exists(complete))
      capacity(chunk.packageLength() + 4096, -1);
    store.update("insert or ignore into evidence_chunks values(?,?)", inboxId, key);
    var assembler = assemblers.get(c + "/" + d);
    if (assembler == null) {
      assembler = new AlarmSnapshotPackageAssembler(device, 8);
      assemblers.put(c + "/" + d, assembler);
    }
    var result = assembler.accept(chunk);
    if (result.isEmpty()) {
      store.update("update inbox set status='assembling' where id=?", inboxId);
      return;
    }
    var p = result.get();
    store.transaction(
        () -> {
          store.update(
              "insert into evidence_receipts(c,d,event_id,hash,path,created) values(?,?,?,?,?,?) on"
                  + " conflict(c,d,event_id,hash) do update set path=excluded.path,deleted=0",
              c,
              d,
              Long.toUnsignedString(p.eventId()),
              p.packageSha256(),
              p.packagePath().toAbsolutePath().toString(),
              Store.now());
          store.schedule(
              c,
              d,
              "ack",
              Map.of(
                  "eventId",
                  Long.toUnsignedString(p.eventId()),
                  "packageSha256",
                  p.packageSha256()),
              "ack:" + key,
              false);
          store.update(
              "update inbox set status='done' where id in(select inbox_id from evidence_chunks"
                  + " where package_key=?)",
              key);
          store.update("delete from evidence_chunks where package_key=?", key);
        });
  }

  public synchronized void prune(int days) throws Exception {
    long before = Store.now() - 86400L * days;
    for (var r :
        store.rows(
            "select r.* from evidence_receipts r join jobs j on"
                + " j.id=('ack:'||r.c||'/'||r.d||'/'||r.event_id||'/'||r.hash) where r.deleted=0"
                + " and r.created<? and j.status='done'",
            before)) {
      Path packagePath = Path.of((String) r.get("path")).toAbsolutePath().normalize();
      if (!packagePath.startsWith(root.toAbsolutePath().normalize())
          || Files.isSymbolicLink(packagePath)
          || !packagePath.getParent().toRealPath().startsWith(root.toRealPath())
          || (Files.exists(packagePath) && !packagePath.toRealPath().startsWith(root.toRealPath())))
        continue;
      Files.deleteIfExists(packagePath);
      Path receipt = packagePath.getParent().resolve("receipt.json");
      if (Files.isRegularFile(receipt, LinkOption.NOFOLLOW_LINKS)
          && Store.JSON
              .readTree(receipt.toFile())
              .path("packageSha256")
              .asText()
              .equals(r.get("hash"))) Files.delete(receipt);
      store.update(
          "update evidence_receipts set deleted=1 where c=? and d=? and event_id=? and hash=?",
          r.get("c"),
          r.get("d"),
          r.get("event_id"),
          r.get("hash"));
    }
    try (var paths = Files.walk(root)) {
      for (var file :
          paths
              .filter(
                  p ->
                      Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)
                          && p.getFileName().toString().matches("image-[0-9]+[.]jpg"))
              .toList())
        if (Files.getLastModifiedTime(file).toMillis() / 1000 < before) Files.delete(file);
    }
  }

  private void capacity(long extra, long excludeInboxId) throws Exception {
    long used = store.pendingEvidenceBytes(excludeInboxId) + store.evidenceReservedBytes();
    if (used + extra > maximum) throw new IllegalStateException("evidence disk capacity exhausted");
  }

  private static void write(Path target, byte[] bytes) throws Exception {
    Path temp = target.resolveSibling(target.getFileName() + ".tmp");
    try (var file =
        FileChannel.open(
            temp,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      var b = ByteBuffer.wrap(bytes);
      while (b.hasRemaining()) file.write(b);
      file.force(true);
    }
    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    try (var directory = FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
      directory.force(true);
    }
  }
}
