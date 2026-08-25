"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const HEADER_LENGTH = 76;
const CHUNK_BYTES = 128 * 1024;
const MAX_PACKAGE_BYTES = 32 * 1024 * 1024;
const TAR_BLOCK_BYTES = 512;

class AlarmSnapshotPackageAssembler {
  constructor(outputDirectory, options = {}) {
    if (!outputDirectory) {
      throw new Error("outputDirectory 不能为空");
    }
    this.maxPendingEvents = options.maxPendingEvents ?? 8;
    if (!Number.isInteger(this.maxPendingEvents) || this.maxPendingEvents < 1) {
      throw new Error("maxPendingEvents 必须大于 0");
    }
    this.outputDirectory = path.resolve(outputDirectory);
    fs.mkdirSync(this.outputDirectory, { recursive: true, mode: 0o700 });
    this.events = new Map();
  }

  accept(chunk) {
    validateChunk(chunk);
    const eventId = String(chunk.eventId);
    const eventDirectory = path.join(this.outputDirectory, eventId);
    fs.mkdirSync(eventDirectory, { recursive: true, mode: 0o700 });
    const hashHex = String(chunk.packageSha256);
    const partialPath = path.join(eventDirectory, `${hashHex}.tar.part`);
    const finalPath = path.join(eventDirectory, `${hashHex}.tar`);

    if (fs.existsSync(finalPath)) {
      validateCompletePackage(finalPath, Number(chunk.packageLength), hashHex);
      const actual = readFileRange(finalPath, Number(chunk.chunkOffset), chunk.chunk.length);
      if (!actual.equals(Buffer.from(chunk.chunk))) {
        throw new Error("已完成抓拍图像包的重复分块冲突");
      }
      writeReceipt(eventDirectory, eventId, hashHex, path.basename(finalPath));
      syncDirectory(eventDirectory);
      this.events.delete(eventId);
      return { eventId, packageSha256: hashHex, packagePath: finalPath };
    }

    let state = this.events.get(eventId);
    if (!state) {
      if (this.events.size >= this.maxPendingEvents) {
        throw new Error("待接收告警抓拍图像事件数量超过有界限制");
      }
      state = {
        packageLength: String(chunk.packageLength),
        packageSha256: hashHex,
        chunkCount: chunk.chunkCount,
        received: new Set(),
      };
      this.events.set(eventId, state);
    }
    if (state.packageLength !== String(chunk.packageLength)
        || state.packageSha256 !== hashHex || state.chunkCount !== chunk.chunkCount) {
      throw new Error("同一 eventId 的抓拍图像包标识冲突");
    }

    const data = Buffer.from(chunk.chunk);
    const descriptor = fs.openSync(partialPath, fs.existsSync(partialPath) ? "r+" : "w+", 0o600);
    try {
      if (state.received.has(chunk.chunkIndex)) {
        const actual = readExact(descriptor, Number(chunk.chunkOffset), data.length);
        if (!actual.equals(data)) {
          throw new Error("重复分块的字节内容冲突");
        }
      } else {
        writeExact(descriptor, Number(chunk.chunkOffset), data);
        fs.fsyncSync(descriptor);
        state.received.add(chunk.chunkIndex);
      }
    } finally {
      fs.closeSync(descriptor);
    }

    if (state.received.size !== state.chunkCount) {
      return null;
    }
    try {
      validateCompletePackage(partialPath, Number(chunk.packageLength), hashHex);
    } catch (error) {
      this.events.delete(eventId);
      fs.rmSync(partialPath, { force: true });
      throw error;
    }
    fs.renameSync(partialPath, finalPath);
    writeReceipt(eventDirectory, eventId, hashHex, path.basename(finalPath));
    syncDirectory(eventDirectory);
    this.events.delete(eventId);
    return { eventId, packageSha256: hashHex, packagePath: finalPath };
  }
}

function validateChunk(chunk) {
  if (!chunk || chunk.messageType !== 2 || chunk.headerLength !== HEADER_LENGTH
      || chunk.packageFormat !== 1 || chunk.evidenceKind !== 1) {
    throw new Error("抓拍图像分块格式不受支持");
  }
  if (!/^[1-9][0-9]*$/.test(String(chunk.eventId))
      || !/^[0-9a-f]{64}$/.test(String(chunk.packageSha256))) {
    throw new Error("抓拍图像包身份非法");
  }
  const packageLength = parseBoundedInteger(chunk.packageLength, "packageLength");
  const chunkOffset = parseBoundedInteger(chunk.chunkOffset, "chunkOffset");
  if (packageLength < 1 || packageLength > MAX_PACKAGE_BYTES) {
    throw new Error("抓拍图像包长度非法");
  }
  const expectedCount = Math.ceil(packageLength / CHUNK_BYTES);
  const expectedOffset = chunk.chunkIndex * CHUNK_BYTES;
  if (!Number.isInteger(chunk.chunkIndex) || !Number.isInteger(chunk.chunkCount)
      || chunk.chunkCount !== expectedCount || chunk.chunkIndex < 0
      || chunk.chunkIndex >= chunk.chunkCount || chunkOffset !== expectedOffset
      || expectedOffset >= packageLength) {
    throw new Error("抓拍图像包分块范围非法");
  }
  const expectedLength = Math.min(CHUNK_BYTES, packageLength - expectedOffset);
  const data = Buffer.from(chunk.chunk);
  if (data.length !== expectedLength) {
    throw new Error("抓拍图像包分块长度非法");
  }
  if (chunk.chunkIndex === 0
      && (data.length < 262 || data.subarray(257, 262).toString("ascii") !== "ustar")) {
    throw new Error("抓拍图像包首块缺少 USTAR 标识");
  }
}

function parseBoundedInteger(value, name) {
  const text = String(value);
  if (!/^(0|[1-9][0-9]*)$/.test(text)) {
    throw new Error(`${name} 不是非负整数`);
  }
  const result = Number(text);
  if (!Number.isSafeInteger(result)) {
    throw new Error(`${name} 超出 JavaScript 安全整数范围`);
  }
  return result;
}

function validateCompletePackage(packagePath, expectedLength, expectedHash) {
  const info = fs.statSync(packagePath);
  if (!info.isFile() || info.size !== expectedLength) {
    throw new Error("重组后的抓拍图像包长度不匹配");
  }
  const digest = crypto.createHash("sha256");
  const descriptor = fs.openSync(packagePath, "r");
  try {
    const buffer = Buffer.allocUnsafe(64 * 1024);
    for (let position = 0; position < expectedLength;) {
      const count = fs.readSync(descriptor, buffer, 0, Math.min(buffer.length, expectedLength - position), position);
      if (count === 0) throw new Error("抓拍图像包提前结束");
      digest.update(buffer.subarray(0, count));
      position += count;
    }
  } finally {
    fs.closeSync(descriptor);
  }
  if (digest.digest("hex") !== expectedHash) {
    throw new Error("重组后的抓拍图像包 SHA-256 不匹配");
  }
  validateSafeUstar(packagePath, expectedLength);
}

function validateSafeUstar(packagePath, packageLength) {
  const descriptor = fs.openSync(packagePath, "r");
  let first = true;
  let terminated = false;
  const names = new Set();
  try {
    for (let offset = 0; offset + TAR_BLOCK_BYTES <= packageLength;) {
      const header = readExact(descriptor, offset, TAR_BLOCK_BYTES);
      if (header.every((value) => value === 0)) {
        if (offset + 2 * TAR_BLOCK_BYTES > packageLength
            || !readExact(descriptor, offset + TAR_BLOCK_BYTES, TAR_BLOCK_BYTES)
              .every((value) => value === 0)) {
          throw new Error("抓拍图像 USTAR 结束块非法");
        }
        terminated = true;
        break;
      }
      if (header.subarray(257, 262).toString("ascii") !== "ustar" || !validTarChecksum(header)) {
        throw new Error("抓拍图像 USTAR Header 非法");
      }
      const basicName = nullTerminatedAscii(header, 0, 100);
      const prefix = nullTerminatedAscii(header, 345, 155);
      const name = prefix ? `${prefix}/${basicName}` : basicName;
      if (first && name !== "manifest.json") {
        throw new Error("抓拍图像 USTAR 的第一项必须是 manifest.json");
      }
      first = false;
      const type = header[156];
      if (!safeTarName(name) || (type !== 0 && type !== 0x30)) {
        throw new Error("抓拍图像 USTAR 包含不安全或非普通文件成员");
      }
      if (names.has(name)) {
        throw new Error("抓拍图像 USTAR 包含重复成员");
      }
      names.add(name);
      const size = parseTarOctal(header, 124, 12);
      const next = offset + TAR_BLOCK_BYTES + Math.ceil(size / TAR_BLOCK_BYTES) * TAR_BLOCK_BYTES;
      if (!Number.isSafeInteger(next) || next > packageLength) {
        throw new Error("抓拍图像 USTAR 成员长度非法");
      }
      offset = next;
    }
  } finally {
    fs.closeSync(descriptor);
  }
  if (first || !terminated) {
    throw new Error("抓拍图像 USTAR 为空或未正确结束");
  }
}

function validTarChecksum(header) {
  const expected = parseTarOctal(header, 148, 8);
  let actual = 0;
  for (let index = 0; index < header.length; index += 1) {
    actual += index >= 148 && index < 156 ? 32 : header[index];
  }
  return expected === actual;
}

function parseTarOctal(header, offset, length) {
  const value = header.subarray(offset, offset + length).toString("ascii").replaceAll("\0", "").trim();
  if (!value) return 0;
  if (!/^[0-7]+$/.test(value)) throw new Error("USTAR 八进制字段非法");
  const result = Number.parseInt(value, 8);
  if (!Number.isSafeInteger(result)) throw new Error("USTAR 数值字段过大");
  return result;
}

function nullTerminatedAscii(header, offset, length) {
  const field = header.subarray(offset, offset + length);
  const end = field.indexOf(0);
  return field.subarray(0, end < 0 ? field.length : end).toString("ascii");
}

function safeTarName(name) {
  if (!name || name.startsWith("/") || name.includes("\\")) return false;
  return name.split("/").every((part) => part && part !== "." && part !== "..");
}

function readFileRange(filePath, offset, length) {
  const descriptor = fs.openSync(filePath, "r");
  try {
    return readExact(descriptor, offset, length);
  } finally {
    fs.closeSync(descriptor);
  }
}

function readExact(descriptor, offset, length) {
  const result = Buffer.alloc(length);
  for (let total = 0; total < length;) {
    const count = fs.readSync(descriptor, result, total, length - total, offset + total);
    if (count === 0) throw new Error("抓拍图像包提前结束");
    total += count;
  }
  return result;
}

function writeExact(descriptor, offset, data) {
  for (let total = 0; total < data.length;) {
    total += fs.writeSync(descriptor, data, total, data.length - total, offset + total);
  }
}

function writeReceipt(eventDirectory, eventId, hashHex, packageName) {
  const receiptPath = path.join(eventDirectory, "receipt.json");
  const temporaryPath = `${receiptPath}.tmp`;
  const payload = Buffer.from(JSON.stringify({
    eventId, kind: "SNAPSHOT", packageSha256: hashHex, package: packageName,
  }));
  const descriptor = fs.openSync(temporaryPath, "w", 0o600);
  try {
    writeExact(descriptor, 0, payload);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temporaryPath, receiptPath);
}

function syncDirectory(directory) {
  const descriptor = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

module.exports = { AlarmSnapshotPackageAssembler };
