package com.inteagle.vdm.service;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.vdm.mqtt.sdk.*;
import com.inteagle.vdm.service.evidence.EvidenceService;
import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EvidencePersistenceTest {
  private static final int CHUNK_BYTES = 128 * 1024;
  @TempDir Path dir;

  @Test
  void acceptsAndPersistsTwoMiBOrdinaryJpeg() throws Exception {
    byte[] raw = new byte[2 * 1024 * 1024];
    raw[0] = 1;
    raw[1] = 8;
    raw[8] = (byte) 0xff;
    raw[9] = (byte) 0xd8;
    raw[raw.length - 2] = (byte) 0xff;
    raw[raw.length - 1] = (byte) 0xd9;
    var frame =
        new VdmCodec(PayloadFormat.JSON)
            .decode("vdm/d/image", VdmTopics.forDevice("d"), raw)
            .value();
    try (var db = new Store(dir, 10, 300)) {
      db.enqueue("a", "d", "vdm/d/image", raw);
      var evidence = new EvidenceService(db, dir.resolve("evidence"), 8 * 1024 * 1024);
      evidence.accept(1, "a", "d", frame);
      assertEquals(raw.length - 8, Files.size(dir.resolve("evidence/a/d/image-1.jpg")));
      assertEquals("done", db.rows("select * from inbox").getFirst().get("status"));
      assertThrows(
          IllegalArgumentException.class, () -> db.enqueue("a", "d", "vdm/d/telemetry", raw));
    }
  }

  @Test
  void recoversPartialPackageAndPersistsAckBeforeRestart() throws Exception {
    byte[] tar = snapshotUstar("frame.jpg");
    try (var db = new Store(dir, 10, 300)) {
      var evidence = new EvidenceService(db, dir.resolve("evidence"), 1048576);
      db.enqueue("a", "d", "vdm/d/image", raw(chunk(tar, 9001, 1)));
      evidence.accept(1, "a", "d", chunk(tar, 9001, 1));
      assertEquals(0, db.rows("select * from jobs").size());
      assertEquals("assembling", db.rows("select * from inbox").getFirst().get("status"));
    }
    try (var db = new Store(dir, 10, 300)) {
      var evidence = new EvidenceService(db, dir.resolve("evidence"), 1048576);
      assertEquals("pending", db.rows("select * from inbox").getFirst().get("status"));
      evidence.accept(1, "a", "d", chunk(tar, 9001, 1));
      db.enqueue("a", "d", "vdm/d/image", raw(chunk(tar, 9001, 0)));
      evidence.accept(2, "a", "d", chunk(tar, 9001, 0));
      assertEquals(1, db.rows("select * from jobs where kind='ack' and status='pending'").size());
      assertEquals(2, db.rows("select * from inbox where status='done'").size());
      assertTrue(Files.exists(dir.resolve("evidence/a/d/9001/receipt.json")));
      String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(tar));
      assertTrue(db.verified("a", "d", "9001", sha));
      db.update("update evidence_receipts set created=0");
      evidence.prune(7);
      assertTrue(db.verified("a", "d", "9001", sha), "pending ACK must retain bytes");
      db.update("update jobs set status='done'");
      evidence.prune(7);
      assertFalse(
          db.verified("a", "d", "9001", sha),
          "finished old evidence removed and no longer ACKable");
      db.update("update jobs set status='pending'");
    }
    try (var db = new Store(dir, 10, 300)) {
      assertEquals(1, db.rows("select * from jobs where status='pending'").size());
    }
  }

  @Test
  void invalidHashCannotCreateAckAndDiskCapRejectsEarly() throws Exception {
    byte[] tar = snapshotUstar("frame.jpg");
    try (var db = new Store(dir, 10, 300)) {
      var evidence = new EvidenceService(db, dir.resolve("evidence"), 1000);
      db.enqueue("a", "d", "vdm/d/image", raw(chunk(tar, 9001, 0)));
      assertThrows(
          IllegalStateException.class, () -> evidence.accept(1, "a", "d", chunk(tar, 9001, 0)));
      assertEquals(0, db.rows("select * from jobs").size());
    }
  }

  static byte[] raw(EvidencePackageChunk p) {
    var b = ByteBuffer.allocate(76 + p.chunk().length);
    b.put((byte) 2)
        .put((byte) 76)
        .put((byte) 1)
        .put((byte) 1)
        .putLong(p.eventId())
        .putLong(p.packageLength())
        .put(p.packageSha256())
        .putInt((int) p.chunkIndex())
        .putInt((int) p.chunkCount())
        .putLong(p.chunkOffset())
        .putInt(p.chunk().length)
        .putInt(0)
        .put(p.chunk());
    return b.array();
  }

  private static EvidencePackageChunk chunk(byte[] packageBytes, long eventId, int index)
      throws Exception {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(packageBytes);
    int count = (packageBytes.length + CHUNK_BYTES - 1) / CHUNK_BYTES;
    int offset = index * CHUNK_BYTES;
    int end = Math.min(offset + CHUNK_BYTES, packageBytes.length);
    return new EvidencePackageChunk(
        2,
        76,
        1,
        1,
        eventId,
        packageBytes.length,
        hash,
        index,
        count,
        offset,
        Arrays.copyOfRange(packageBytes, offset, end));
  }

  private static byte[] snapshotUstar(String imageName) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    writeEntry(output, "manifest.json", "{\"eventId\":\"9001\"}".getBytes(StandardCharsets.UTF_8));
    byte[] image = new byte[140_004];
    image[0] = (byte) 0xff;
    image[1] = (byte) 0xd8;
    image[image.length - 2] = (byte) 0xff;
    image[image.length - 1] = (byte) 0xd9;
    writeEntry(output, imageName, image);
    output.write(new byte[1024]);
    return output.toByteArray();
  }

  private static void writeEntry(ByteArrayOutputStream output, String name, byte[] data)
      throws IOException {
    byte[] header = new byte[512];
    byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(nameBytes, 0, header, 0, nameBytes.length);
    writeOctal(header, 100, 8, 0600);
    writeOctal(header, 108, 8, 0);
    writeOctal(header, 116, 8, 0);
    writeOctal(header, 124, 12, data.length);
    writeOctal(header, 136, 12, 0);
    Arrays.fill(header, 148, 156, (byte) ' ');
    header[156] = '0';
    System.arraycopy("ustar\0".getBytes(StandardCharsets.US_ASCII), 0, header, 257, 6);
    System.arraycopy("00".getBytes(StandardCharsets.US_ASCII), 0, header, 263, 2);
    long checksum = 0;
    for (byte value : header) {
      checksum += Byte.toUnsignedInt(value);
    }
    byte[] checksumBytes = String.format("%06o", checksum).getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(checksumBytes, 0, header, 148, checksumBytes.length);
    header[154] = 0;
    header[155] = ' ';
    output.write(header);
    output.write(data);
    int padding = (512 - data.length % 512) % 512;
    output.write(new byte[padding]);
  }

  private static void writeOctal(byte[] header, int offset, int length, long value) {
    byte[] encoded =
        String.format("%0" + (length - 1) + "o", value).getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(encoded, 0, header, offset, encoded.length);
    header[offset + length - 1] = 0;
  }
}
