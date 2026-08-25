package com.inteagle.vdm.mqtt.sdk;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** 类型 2 告警抓拍图像包的有界、磁盘优先重组器。 */
public final class AlarmSnapshotPackageAssembler {
  private static final int HEADER_LENGTH = 76;
  private static final int CHUNK_BYTES = 128 * 1024;
  private static final long MAX_PACKAGE_BYTES = 32L * 1024 * 1024;
  private static final int TAR_BLOCK_BYTES = 512;

  private static final class PackageState {
    private final long packageLength;
    private final byte[] packageSha256;
    private final long chunkCount;
    private final Set<Long> received = new HashSet<>();

    private PackageState(EvidencePackageChunk chunk) {
      packageLength = chunk.packageLength();
      packageSha256 = chunk.packageSha256();
      chunkCount = chunk.chunkCount();
    }

    private boolean matches(EvidencePackageChunk chunk) {
      return packageLength == chunk.packageLength()
          && chunkCount == chunk.chunkCount()
          && Arrays.equals(packageSha256, chunk.packageSha256());
    }
  }

  private final Path outputDirectory;
  private final int maxPendingEvents;
  private final Map<Long, PackageState> events = new HashMap<>();

  public AlarmSnapshotPackageAssembler(Path outputDirectory, int maxPendingEvents)
      throws IOException {
    if (maxPendingEvents < 1) {
      throw new IllegalArgumentException("maxPendingEvents 必须大于 0");
    }
    this.outputDirectory = outputDirectory.toAbsolutePath().normalize();
    this.maxPendingEvents = maxPendingEvents;
    Files.createDirectories(this.outputDirectory);
  }

  public synchronized Optional<CompletedAlarmSnapshotPackage> accept(
      EvidencePackageChunk chunk) throws IOException {
    validateChunk(chunk);
    Path eventDirectory = outputDirectory.resolve(Long.toUnsignedString(chunk.eventId()));
    Files.createDirectories(eventDirectory);
    String hashHex = HexFormat.of().formatHex(chunk.packageSha256());
    Path partialPath = eventDirectory.resolve(hashHex + ".tar.part");
    Path finalPath = eventDirectory.resolve(hashHex + ".tar");

    if (Files.isRegularFile(finalPath)) {
      verifyCompletedDuplicate(finalPath, chunk);
      writeReceipt(eventDirectory, chunk.eventId(), hashHex, finalPath.getFileName().toString());
      syncDirectory(eventDirectory);
      events.remove(chunk.eventId());
      return Optional.of(new CompletedAlarmSnapshotPackage(chunk.eventId(), hashHex, finalPath));
    }

    PackageState state = events.get(chunk.eventId());
    if (state == null) {
      if (events.size() >= maxPendingEvents) {
        throw new IOException("待接收告警抓拍图像事件数量超过有界限制");
      }
      state = new PackageState(chunk);
      events.put(chunk.eventId(), state);
    }
    if (!state.matches(chunk)) {
      throw new IOException("同一 eventId 的抓拍图像包标识冲突");
    }

    try (FileChannel output = FileChannel.open(
        partialPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      if (state.received.contains(chunk.chunkIndex())) {
        byte[] actual = readAt(output, chunk.chunkOffset(), chunk.chunk().length);
        if (!Arrays.equals(actual, chunk.chunk())) {
          throw new IOException("重复分块的字节内容冲突");
        }
      } else {
        writeAt(output, chunk.chunkOffset(), chunk.chunk());
        output.force(true);
        state.received.add(chunk.chunkIndex());
      }
    }

    if (state.received.size() != state.chunkCount) {
      return Optional.empty();
    }

    try {
      validateCompletePackage(partialPath, state.packageLength, state.packageSha256);
    } catch (IOException exception) {
      events.remove(chunk.eventId());
      Files.deleteIfExists(partialPath);
      throw exception;
    }
    atomicMove(partialPath, finalPath);
    writeReceipt(eventDirectory, chunk.eventId(), hashHex, finalPath.getFileName().toString());
    syncDirectory(eventDirectory);
    events.remove(chunk.eventId());
    return Optional.of(new CompletedAlarmSnapshotPackage(chunk.eventId(), hashHex, finalPath));
  }

  private static void validateChunk(EvidencePackageChunk chunk) throws IOException {
    if (chunk == null) {
      throw new IllegalArgumentException("抓拍图像分块不能为空");
    }
    if (chunk.messageType() != 2 || chunk.headerLength() != HEADER_LENGTH
        || chunk.packageFormat() != 1 || chunk.evidenceKind() != 1) {
      throw new IOException("抓拍图像分块格式不受支持");
    }
    if (chunk.eventId() == 0 || chunk.packageLength() < 1
        || chunk.packageLength() > MAX_PACKAGE_BYTES) {
      throw new IOException("抓拍图像包身份或长度非法");
    }
    long expectedCount = (chunk.packageLength() + CHUNK_BYTES - 1) / CHUNK_BYTES;
    long expectedOffset = chunk.chunkIndex() * CHUNK_BYTES;
    if (chunk.chunkCount() != expectedCount || chunk.chunkIndex() < 0
        || chunk.chunkIndex() >= chunk.chunkCount() || chunk.chunkOffset() != expectedOffset
        || expectedOffset >= chunk.packageLength()) {
      throw new IOException("抓拍图像包分块范围非法");
    }
    int expectedLength = (int) Math.min(CHUNK_BYTES, chunk.packageLength() - expectedOffset);
    if (chunk.chunk().length != expectedLength) {
      throw new IOException("抓拍图像包分块长度非法");
    }
    byte[] bytes = chunk.chunk();
    if (chunk.chunkIndex() == 0
        && (bytes.length < 262
            || !Arrays.equals(Arrays.copyOfRange(bytes, 257, 262), "ustar".getBytes(StandardCharsets.US_ASCII)))) {
      throw new IOException("抓拍图像包首块缺少 USTAR 标识");
    }
  }

  private static void verifyCompletedDuplicate(Path finalPath, EvidencePackageChunk chunk)
      throws IOException {
    validateCompletePackage(finalPath, chunk.packageLength(), chunk.packageSha256());
    try (FileChannel input = FileChannel.open(finalPath, StandardOpenOption.READ)) {
      byte[] actual = readAt(input, chunk.chunkOffset(), chunk.chunk().length);
      if (!Arrays.equals(actual, chunk.chunk())) {
        throw new IOException("已完成抓拍图像包的重复分块冲突");
      }
    }
  }

  private static void validateCompletePackage(
      Path packagePath, long expectedLength, byte[] expectedSha256) throws IOException {
    if (Files.size(packagePath) != expectedLength) {
      throw new IOException("重组后的抓拍图像包长度不匹配");
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException(exception);
    }
    try (var input = Files.newInputStream(packagePath)) {
      byte[] buffer = new byte[64 * 1024];
      for (int read; (read = input.read(buffer)) >= 0;) {
        if (read > 0) {
          digest.update(buffer, 0, read);
        }
      }
    }
    if (!Arrays.equals(digest.digest(), expectedSha256)) {
      throw new IOException("重组后的抓拍图像包 SHA-256 不匹配");
    }
    validateSafeUstar(packagePath);
  }

  private static void validateSafeUstar(Path packagePath) throws IOException {
    long packageLength = Files.size(packagePath);
    boolean first = true;
    boolean terminated = false;
    Set<String> names = new HashSet<>();
    try (FileChannel input = FileChannel.open(packagePath, StandardOpenOption.READ)) {
      long offset = 0;
      while (offset + TAR_BLOCK_BYTES <= packageLength) {
        byte[] header = readAt(input, offset, TAR_BLOCK_BYTES);
        if (allZero(header)) {
          if (offset + 2L * TAR_BLOCK_BYTES > packageLength
              || !allZero(readAt(input, offset + TAR_BLOCK_BYTES, TAR_BLOCK_BYTES))) {
            throw new IOException("抓拍图像 USTAR 结束块非法");
          }
          terminated = true;
          break;
        }
        if (!ascii(header, 257, 5).equals("ustar") || !validTarChecksum(header)) {
          throw new IOException("抓拍图像 USTAR Header 非法");
        }
        String name = tarName(header);
        if (first && !name.equals("manifest.json")) {
          throw new IOException("抓拍图像 USTAR 的第一项必须是 manifest.json");
        }
        first = false;
        int type = Byte.toUnsignedInt(header[156]);
        if (!safeTarName(name) || (type != 0 && type != '0')) {
          throw new IOException("抓拍图像 USTAR 包含不安全或非普通文件成员");
        }
        if (!names.add(name)) {
          throw new IOException("抓拍图像 USTAR 包含重复成员");
        }
        long size = parseTarOctal(header, 124, 12);
        long next = offset + TAR_BLOCK_BYTES + ((size + TAR_BLOCK_BYTES - 1) / TAR_BLOCK_BYTES) * TAR_BLOCK_BYTES;
        if (size < 0 || next < offset || next > packageLength) {
          throw new IOException("抓拍图像 USTAR 成员长度非法");
        }
        offset = next;
      }
    }
    if (first || !terminated) {
      throw new IOException("抓拍图像 USTAR 为空或未正确结束");
    }
  }

  private static String tarName(byte[] header) {
    String name = nullTerminatedAscii(header, 0, 100);
    String prefix = nullTerminatedAscii(header, 345, 155);
    return prefix.isEmpty() ? name : prefix + "/" + name;
  }

  private static boolean safeTarName(String name) {
    if (name.isEmpty() || name.startsWith("/") || name.contains("\\")) {
      return false;
    }
    String[] parts = name.split("/", -1);
    for (String part : parts) {
      if (part.isEmpty() || part.equals(".") || part.equals("..")) {
        return false;
      }
    }
    return true;
  }

  private static boolean validTarChecksum(byte[] header) throws IOException {
    long expected = parseTarOctal(header, 148, 8);
    long actual = 0;
    for (int index = 0; index < header.length; index++) {
      actual += index >= 148 && index < 156 ? 32 : Byte.toUnsignedInt(header[index]);
    }
    return expected == actual;
  }

  private static long parseTarOctal(byte[] bytes, int offset, int length) throws IOException {
    String value = ascii(bytes, offset, length).replace("\0", "").trim();
    if (value.isEmpty()) {
      return 0;
    }
    try {
      return Long.parseLong(value, 8);
    } catch (NumberFormatException exception) {
      throw new IOException("USTAR 八进制字段非法", exception);
    }
  }

  private static String nullTerminatedAscii(byte[] bytes, int offset, int length) {
    int end = offset;
    while (end < offset + length && bytes[end] != 0) {
      end++;
    }
    return ascii(bytes, offset, end - offset);
  }

  private static String ascii(byte[] bytes, int offset, int length) {
    return new String(bytes, offset, length, StandardCharsets.US_ASCII);
  }

  private static boolean allZero(byte[] bytes) {
    for (byte value : bytes) {
      if (value != 0) {
        return false;
      }
    }
    return true;
  }

  private static byte[] readAt(FileChannel channel, long offset, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    long position = offset;
    while (buffer.hasRemaining()) {
      int read = channel.read(buffer, position);
      if (read < 0) {
        throw new EOFException("抓拍图像包提前结束");
      }
      position += read;
    }
    return buffer.array();
  }

  private static void writeAt(FileChannel channel, long offset, byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long position = offset;
    while (buffer.hasRemaining()) {
      position += channel.write(buffer, position);
    }
  }

  private static void writeReceipt(
      Path eventDirectory, long eventId, String hashHex, String packageName) throws IOException {
    String payload = "{\"eventId\":\"" + Long.toUnsignedString(eventId)
        + "\",\"kind\":\"SNAPSHOT\",\"packageSha256\":\"" + hashHex
        + "\",\"package\":\"" + packageName + "\"}";
    Path temporaryPath = eventDirectory.resolve("receipt.json.tmp");
    Path receiptPath = eventDirectory.resolve("receipt.json");
    try (FileChannel output = FileChannel.open(
        temporaryPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE)) {
      ByteBuffer buffer = StandardCharsets.UTF_8.encode(payload);
      while (buffer.hasRemaining()) {
        output.write(buffer);
      }
      output.force(true);
    }
    atomicMove(temporaryPath, receiptPath);
  }

  private static void atomicMove(Path source, Path target) throws IOException {
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException exception) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }
}
