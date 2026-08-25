package sdk

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func snapshotUSTAR(t *testing.T, unsafe bool) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	members := []struct {
		name string
		data []byte
	}{
		{"manifest.json", []byte(`{"eventId":"9001"}`)},
		{"frame-000.jpg", append([]byte{0xff, 0xd8}, bytes.Repeat([]byte{'x'}, 140_000)...)},
	}
	if unsafe {
		members[1].name = "../outside.jpg"
	}
	for _, member := range members {
		header := &tar.Header{Name: member.name, Mode: 0o600, Size: int64(len(member.data))}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(member.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func snapshotChunk(packageBytes []byte, eventID uint64, index uint32) *EvidencePackageChunk {
	hash := sha256.Sum256(packageBytes)
	count := uint32((len(packageBytes) + evidenceChunkBytes - 1) / evidenceChunkBytes)
	offset := int(index) * evidenceChunkBytes
	end := min(offset+evidenceChunkBytes, len(packageBytes))
	return &EvidencePackageChunk{
		MessageType: 2, HeaderLength: 76, PackageFormat: 1, EvidenceKind: 1,
		EventID: eventID, PackageLength: uint64(len(packageBytes)), PackageSHA256: hash,
		ChunkIndex: index, ChunkCount: count, ChunkOffset: uint64(offset),
		Chunk: append([]byte(nil), packageBytes[offset:end]...),
	}
}

func TestAlarmSnapshotPackageAssemblerReordersPersistsAndIsIdempotent(t *testing.T) {
	packageBytes := snapshotUSTAR(t, false)
	assembler, err := NewAlarmSnapshotPackageAssembler(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	second := snapshotChunk(packageBytes, 9001, 1)
	if completed, err := assembler.Accept(second); err != nil || completed != nil {
		t.Fatalf("unexpected first result completed=%v err=%v", completed, err)
	}
	if completed, err := assembler.Accept(second); err != nil || completed != nil {
		t.Fatalf("duplicate must be idempotent completed=%v err=%v", completed, err)
	}
	completed, err := assembler.Accept(snapshotChunk(packageBytes, 9001, 0))
	if err != nil || completed == nil {
		t.Fatalf("expected completion: completed=%v err=%v", completed, err)
	}
	actual, err := os.ReadFile(completed.PackagePath)
	if err != nil || !bytes.Equal(actual, packageBytes) {
		t.Fatalf("persisted package mismatch err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(completed.PackagePath), "receipt.json")); err != nil {
		t.Fatalf("receipt missing: %v", err)
	}
	if repeated, err := assembler.Accept(second); err != nil || repeated == nil {
		t.Fatalf("completed duplicate must remain idempotent result=%v err=%v", repeated, err)
	}
}

func TestAlarmSnapshotPackageAssemblerRejectsConflictAndUnsafeUSTAR(t *testing.T) {
	packageBytes := snapshotUSTAR(t, false)
	assembler, _ := NewAlarmSnapshotPackageAssembler(t.TempDir(), 2)
	first := snapshotChunk(packageBytes, 9002, 0)
	if _, err := assembler.Accept(first); err != nil {
		t.Fatal(err)
	}
	conflict := snapshotChunk(packageBytes, 9002, 0)
	conflict.Chunk[0] ^= 0xff
	if _, err := assembler.Accept(conflict); err == nil {
		t.Fatal("conflicting duplicate must be rejected")
	}

	unsafePackage := snapshotUSTAR(t, true)
	unsafeAssembler, _ := NewAlarmSnapshotPackageAssembler(t.TempDir(), 1)
	var completionErr error
	for index := uint32(0); index < snapshotChunk(unsafePackage, 9003, 0).ChunkCount; index++ {
		_, completionErr = unsafeAssembler.Accept(snapshotChunk(unsafePackage, 9003, index))
	}
	if completionErr == nil {
		t.Fatal("unsafe USTAR member must be rejected")
	}
}

func TestAlarmSnapshotPackageAssemblerBoundsPendingEvents(t *testing.T) {
	packageBytes := snapshotUSTAR(t, false)
	assembler, _ := NewAlarmSnapshotPackageAssembler(t.TempDir(), 1)
	if _, err := assembler.Accept(snapshotChunk(packageBytes, 9101, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := assembler.Accept(snapshotChunk(packageBytes, 9102, 0)); err == nil {
		t.Fatal("pending event bound must be enforced")
	}
}
