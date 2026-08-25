package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class AlarmSnapshotPackageAssemblerTest {
  private static final int CHUNK_BYTES = 128 * 1024;

  @TempDir Path temporaryDirectory;

  @Test
  void reordersPersistsAndTreatsDuplicatesAsIdempotent() throws Exception {
    byte[] packageBytes = snapshotUstar("frame-000.jpg");
    AlarmSnapshotPackageAssembler assembler =
        new AlarmSnapshotPackageAssembler(temporaryDirectory, 2);
    EvidencePackageChunk second = chunk(packageBytes, 9001L, 1);

    assertTrue(assembler.accept(second).isEmpty());
    assertTrue(assembler.accept(second).isEmpty());
    Optional<CompletedAlarmSnapshotPackage> result =
        assembler.accept(chunk(packageBytes, 9001L, 0));
    assertTrue(result.isPresent());
    assertArrayEquals(packageBytes, Files.readAllBytes(result.orElseThrow().packagePath()));
    assertTrue(Files.isRegularFile(result.orElseThrow().packagePath().getParent().resolve("receipt.json")));

    Optional<CompletedAlarmSnapshotPackage> repeated = assembler.accept(second);
    assertTrue(repeated.isPresent());
    assertEquals(result.orElseThrow().packageSha256(), repeated.orElseThrow().packageSha256());
  }

  @Test
  void rejectsConflictingDuplicateAndUnsafeUstar() throws Exception {
    byte[] packageBytes = snapshotUstar("frame-000.jpg");
    AlarmSnapshotPackageAssembler assembler =
        new AlarmSnapshotPackageAssembler(temporaryDirectory.resolve("conflict"), 2);
    EvidencePackageChunk first = chunk(packageBytes, 9002L, 0);
    assertTrue(assembler.accept(first).isEmpty());
    byte[] conflictingBytes = first.chunk();
    conflictingBytes[0] ^= (byte) 0xff;
    EvidencePackageChunk conflicting = new EvidencePackageChunk(
        first.messageType(), first.headerLength(), first.packageFormat(), first.evidenceKind(),
        first.eventId(), first.packageLength(), first.packageSha256(), first.chunkIndex(),
        first.chunkCount(), first.chunkOffset(), conflictingBytes);
    assertThrows(IOException.class, () -> assembler.accept(conflicting));

    byte[] unsafe = snapshotUstar("../outside.jpg");
    AlarmSnapshotPackageAssembler unsafeAssembler =
        new AlarmSnapshotPackageAssembler(temporaryDirectory.resolve("unsafe"), 1);
    assertTrue(unsafeAssembler.accept(chunk(unsafe, 9003L, 0)).isEmpty());
    assertThrows(IOException.class, () -> unsafeAssembler.accept(chunk(unsafe, 9003L, 1)));
  }

  @Test
  void boundsPendingEvents() throws Exception {
    byte[] packageBytes = snapshotUstar("frame-000.jpg");
    AlarmSnapshotPackageAssembler assembler =
        new AlarmSnapshotPackageAssembler(temporaryDirectory, 1);
    assertFalse(assembler.accept(chunk(packageBytes, 9101L, 0)).isPresent());
    assertThrows(IOException.class, () -> assembler.accept(chunk(packageBytes, 9102L, 0)));
  }

  private static EvidencePackageChunk chunk(byte[] packageBytes, long eventId, int index)
      throws Exception {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(packageBytes);
    int count = (packageBytes.length + CHUNK_BYTES - 1) / CHUNK_BYTES;
    int offset = index * CHUNK_BYTES;
    int end = Math.min(offset + CHUNK_BYTES, packageBytes.length);
    return new EvidencePackageChunk(
        2, 76, 1, 1, eventId, packageBytes.length, hash,
        index, count, offset, Arrays.copyOfRange(packageBytes, offset, end));
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
    byte[] encoded = String.format("%0" + (length - 1) + "o", value)
        .getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(encoded, 0, header, offset, encoded.length);
    header[offset + length - 1] = 0;
  }
}
