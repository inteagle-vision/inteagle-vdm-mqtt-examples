package sdk

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// CompletedAlarmSnapshotPackage 是通过完整性与安全校验后的告警抓拍图像包。
type CompletedAlarmSnapshotPackage struct {
	EventID       uint64
	PackageSHA256 string
	PackagePath   string
}

type snapshotPackageState struct {
	packageLength uint64
	packageSHA256 [32]byte
	chunkCount    uint32
	received      map[uint32]struct{}
}

// AlarmSnapshotPackageAssembler 将类型 2 分块有界、磁盘优先地重组为 USTAR 图像包。
// 同一实例可并发调用；只有完整包、receipt.json 和目录元数据均持久化后才返回完成结果。
type AlarmSnapshotPackageAssembler struct {
	outputDir        string
	maxPendingEvents int
	mu               sync.Mutex
	events           map[uint64]*snapshotPackageState
}

func NewAlarmSnapshotPackageAssembler(outputDir string, maxPendingEvents int) (*AlarmSnapshotPackageAssembler, error) {
	if strings.TrimSpace(outputDir) == "" {
		return nil, errors.New("outputDir 不能为空")
	}
	if maxPendingEvents < 1 {
		return nil, errors.New("maxPendingEvents 必须大于 0")
	}
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return nil, fmt.Errorf("创建抓拍图像目录失败: %w", err)
	}
	return &AlarmSnapshotPackageAssembler{
		outputDir: outputDir, maxPendingEvents: maxPendingEvents,
		events: make(map[uint64]*snapshotPackageState),
	}, nil
}

func (a *AlarmSnapshotPackageAssembler) Accept(chunk *EvidencePackageChunk) (*CompletedAlarmSnapshotPackage, error) {
	if err := validateSnapshotChunk(chunk); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	eventDir := filepath.Join(a.outputDir, strconv.FormatUint(chunk.EventID, 10))
	if err := os.MkdirAll(eventDir, 0o700); err != nil {
		return nil, fmt.Errorf("创建事件目录失败: %w", err)
	}
	hashHex := hex.EncodeToString(chunk.PackageSHA256[:])
	partPath := filepath.Join(eventDir, hashHex+".tar.part")
	finalPath := filepath.Join(eventDir, hashHex+".tar")

	if completed, err := completedSnapshotPackage(finalPath, eventDir, chunk, hashHex); err != nil {
		return nil, err
	} else if completed != nil {
		delete(a.events, chunk.EventID)
		return completed, nil
	}

	state := a.events[chunk.EventID]
	if state == nil {
		if len(a.events) >= a.maxPendingEvents {
			return nil, errors.New("待接收告警抓拍图像事件数量超过有界限制")
		}
		state = &snapshotPackageState{
			packageLength: chunk.PackageLength,
			packageSHA256: chunk.PackageSHA256,
			chunkCount:    chunk.ChunkCount,
			received:      make(map[uint32]struct{}),
		}
		a.events[chunk.EventID] = state
	}
	if state.packageLength != chunk.PackageLength || state.packageSHA256 != chunk.PackageSHA256 || state.chunkCount != chunk.ChunkCount {
		return nil, errors.New("同一 eventId 的抓拍图像包标识冲突")
	}

	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("打开抓拍图像临时包失败: %w", err)
	}
	defer file.Close()
	if _, duplicate := state.received[chunk.ChunkIndex]; duplicate {
		actual := make([]byte, len(chunk.Chunk))
		if _, err := file.ReadAt(actual, int64(chunk.ChunkOffset)); err != nil && err != io.EOF {
			return nil, fmt.Errorf("读取重复分块失败: %w", err)
		}
		if !bytes.Equal(actual, chunk.Chunk) {
			return nil, errors.New("重复分块的字节内容冲突")
		}
	} else {
		if _, err := file.WriteAt(chunk.Chunk, int64(chunk.ChunkOffset)); err != nil {
			return nil, fmt.Errorf("写入抓拍图像分块失败: %w", err)
		}
		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("持久化抓拍图像分块失败: %w", err)
		}
		state.received[chunk.ChunkIndex] = struct{}{}
	}
	if len(state.received) != int(state.chunkCount) {
		return nil, nil
	}
	if err := file.Close(); err != nil {
		return nil, err
	}

	if err := validateCompleteSnapshotPackage(partPath, state.packageLength, state.packageSHA256); err != nil {
		delete(a.events, chunk.EventID)
		_ = os.Remove(partPath)
		return nil, err
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		return nil, fmt.Errorf("提交抓拍图像包失败: %w", err)
	}
	if err := writeSnapshotReceipt(eventDir, chunk.EventID, hashHex, filepath.Base(finalPath)); err != nil {
		return nil, err
	}
	if err := syncDirectory(eventDir); err != nil {
		return nil, err
	}
	delete(a.events, chunk.EventID)
	return &CompletedAlarmSnapshotPackage{EventID: chunk.EventID, PackageSHA256: hashHex, PackagePath: finalPath}, nil
}

func validateSnapshotChunk(chunk *EvidencePackageChunk) error {
	if chunk == nil {
		return errors.New("抓拍图像分块不能为空")
	}
	if chunk.MessageType != 2 || chunk.HeaderLength != evidencePackageHeaderLength || chunk.PackageFormat != 1 || chunk.EvidenceKind != 1 {
		return errors.New("抓拍图像分块格式不受支持")
	}
	if chunk.EventID == 0 || chunk.PackageLength == 0 || chunk.PackageLength > maxEvidencePackageBytes {
		return errors.New("抓拍图像包身份或长度非法")
	}
	expectedCount := uint32((chunk.PackageLength + evidenceChunkBytes - 1) / evidenceChunkBytes)
	expectedOffset := uint64(chunk.ChunkIndex) * evidenceChunkBytes
	if chunk.ChunkCount != expectedCount || chunk.ChunkIndex >= chunk.ChunkCount || chunk.ChunkOffset != expectedOffset || expectedOffset >= chunk.PackageLength {
		return errors.New("抓拍图像包分块范围非法")
	}
	expectedLength := min(uint64(evidenceChunkBytes), chunk.PackageLength-expectedOffset)
	if uint64(len(chunk.Chunk)) != expectedLength {
		return errors.New("抓拍图像包分块长度非法")
	}
	if chunk.ChunkIndex == 0 && (len(chunk.Chunk) < 262 || string(chunk.Chunk[257:262]) != "ustar") {
		return errors.New("抓拍图像包首块缺少 USTAR 标识")
	}
	return nil
}

func completedSnapshotPackage(finalPath, eventDir string, chunk *EvidencePackageChunk, hashHex string) (*CompletedAlarmSnapshotPackage, error) {
	info, err := os.Stat(finalPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if uint64(info.Size()) != chunk.PackageLength {
		return nil, errors.New("已完成抓拍图像包长度冲突")
	}
	file, err := os.Open(finalPath)
	if err != nil {
		return nil, err
	}
	actual := make([]byte, len(chunk.Chunk))
	_, readErr := file.ReadAt(actual, int64(chunk.ChunkOffset))
	closeErr := file.Close()
	if readErr != nil && readErr != io.EOF {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if !bytes.Equal(actual, chunk.Chunk) {
		return nil, errors.New("已完成抓拍图像包的重复分块冲突")
	}
	if err := validateCompleteSnapshotPackage(finalPath, chunk.PackageLength, chunk.PackageSHA256); err != nil {
		return nil, err
	}
	if err := writeSnapshotReceipt(eventDir, chunk.EventID, hashHex, filepath.Base(finalPath)); err != nil {
		return nil, err
	}
	if err := syncDirectory(eventDir); err != nil {
		return nil, err
	}
	return &CompletedAlarmSnapshotPackage{EventID: chunk.EventID, PackageSHA256: hashHex, PackagePath: finalPath}, nil
}

func validateCompleteSnapshotPackage(packagePath string, expectedLength uint64, expectedHash [32]byte) error {
	info, err := os.Stat(packagePath)
	if err != nil {
		return err
	}
	if uint64(info.Size()) != expectedLength {
		return errors.New("重组后的抓拍图像包长度不匹配")
	}
	file, err := os.Open(packagePath)
	if err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if !bytes.Equal(hash.Sum(nil), expectedHash[:]) {
		return errors.New("重组后的抓拍图像包 SHA-256 不匹配")
	}
	return validateSafeUSTAR(packagePath)
}

func validateSafeUSTAR(packagePath string) error {
	file, err := os.Open(packagePath)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := tar.NewReader(file)
	first := true
	seen := make(map[string]struct{})
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("抓拍图像 USTAR 非法: %w", err)
		}
		name := header.Name
		if first && name != "manifest.json" {
			return errors.New("抓拍图像 USTAR 的第一项必须是 manifest.json")
		}
		first = false
		if !safeUSTARName(name) || (header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA) {
			return errors.New("抓拍图像 USTAR 包含不安全或非普通文件成员")
		}
		if _, duplicate := seen[name]; duplicate {
			return errors.New("抓拍图像 USTAR 包含重复成员")
		}
		seen[name] = struct{}{}
	}
	if first {
		return errors.New("抓拍图像 USTAR 不能为空")
	}
	return nil
}

func safeUSTARName(name string) bool {
	if name == "" || strings.Contains(name, "\\") || pathpkg.IsAbs(name) {
		return false
	}
	clean := pathpkg.Clean(name)
	return clean == name && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func writeSnapshotReceipt(eventDir string, eventID uint64, hashHex, packageName string) error {
	receipt := map[string]string{
		"eventId": strconv.FormatUint(eventID, 10), "kind": "SNAPSHOT",
		"packageSha256": hashHex, "package": packageName,
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporaryPath := filepath.Join(eventDir, "receipt.json.tmp")
	receiptPath := filepath.Join(eventDir, "receipt.json")
	file, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(payload); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, receiptPath); err != nil {
		return err
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
