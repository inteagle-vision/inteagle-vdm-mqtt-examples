package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/sdk"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func diskBytes(root string) (int64, error) {
	var total int64
	e := filepath.Walk(root, func(_ string, info os.FileInfo, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, e
}
func (s *Service) evidenceCapacity(additional int64) error {
	used, e := diskBytes(filepath.Join(s.dir, "evidence"))
	if e != nil {
		return e
	}
	var chunks int64
	if e = s.db.QueryRow("SELECT COALESCE(sum(length(payload)),0) FROM evidence_chunks").Scan(&chunks); e != nil {
		return e
	}
	if used+chunks+additional > s.config.MaxEvidenceBytes {
		return ErrOverloaded
	}
	return nil
}
func (s *Service) processImage(ctx context.Context, row inboxItem, decoded *sdk.DecodedPayload) error {
	s.evidenceMu.Lock()
	defer s.evidenceMu.Unlock()
	switch frame := decoded.Value.(type) {
	case *sdk.ImageFrame:
		return s.saveJPEG(ctx, row, frame)
	case *sdk.EvidencePackageChunk:
		return s.saveChunk(ctx, row, frame)
	default:
		return errors.New("unknown image type")
	}
}
func (s *Service) saveJPEG(ctx context.Context, row inboxItem, frame *sdk.ImageFrame) error {
	hash := sha256.Sum256(row.payload)
	dir := filepath.Join(s.evidenceDir(row.connection, row.device), "jpeg")
	path := filepath.Join(dir, hex.EncodeToString(hash[:])+".jpg")
	if _, e := os.Stat(path); errors.Is(e, os.ErrNotExist) {
		if e = s.evidenceCapacity(int64(len(frame.JPEG))); e != nil {
			return e
		}
		if e = os.MkdirAll(dir, 0700); e != nil {
			return e
		}
		if e = durableFile(path, frame.JPEG); e != nil {
			return e
		}
	} else if e != nil {
		return e
	}
	_, e := s.db.ExecContext(ctx, "UPDATE inbox SET status='done',error=NULL WHERE id=?", row.id)
	return e
}
func durableFile(path string, payload []byte) error {
	f, e := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(payload); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(path+".tmp", path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func (s *Service) saveChunk(ctx context.Context, row inboxItem, chunk *sdk.EvidencePackageChunk) error {
	event := strconv.FormatUint(chunk.EventID, 10)
	hash := hex.EncodeToString(chunk.PackageSHA256[:])
	dir := s.evidenceDir(row.connection, row.device)
	base := filepath.Join(dir, event, hash+".tar")
	additional := int64(chunk.PackageLength)
	for _, p := range []string{base, base + ".part"} {
		if info, e := os.Stat(p); e == nil {
			additional -= info.Size()
		}
	}
	if additional < 0 {
		additional = 0
	}
	var existing []byte
	e := s.db.QueryRow("SELECT payload FROM evidence_chunks WHERE connection=? AND device=? AND event_id=? AND hash=? AND chunk_index=?", row.connection, row.device, event, hash, chunk.ChunkIndex).Scan(&existing)
	if e == nil {
		if string(existing) != string(row.payload) {
			return errors.New("conflicting durable duplicate evidence chunk")
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	} else {
		additional += int64(len(row.payload))
	}
	if e = s.evidenceCapacity(additional + 1024); e != nil {
		return e
	}
	if _, e = s.db.ExecContext(ctx, "INSERT OR IGNORE INTO evidence_chunks(connection,device,event_id,hash,chunk_index,payload,created) VALUES(?,?,?,?,?,?,?)", row.connection, row.device, event, hash, chunk.ChunkIndex, row.payload, time.Now().Unix()); e != nil {
		return e
	}
	// Rebuild the assembler's bounded in-memory bitmap from durable chunks. Thus a
	// restart after any individual PUBACK never loses knowledge of a received chunk.
	assembler, e := sdk.NewAlarmSnapshotPackageAssembler(dir, 1)
	if e != nil {
		return e
	}
	rows, e := s.db.QueryContext(ctx, "SELECT payload FROM evidence_chunks WHERE connection=? AND device=? AND event_id=? AND hash=? ORDER BY chunk_index", row.connection, row.device, event, hash)
	if e != nil {
		return e
	}
	topics, _ := sdk.TopicsForDevice(row.device)
	codec := sdk.Codec{Format: sdk.PayloadFormat(s.devices[row.connection+"/"+row.device].Format)}
	var complete *sdk.CompletedAlarmSnapshotPackage
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			break
		}
		var decoded *sdk.DecodedPayload
		decoded, e = codec.Decode(topics.Topic("image"), topics, raw)
		if e != nil {
			break
		}
		complete, e = assembler.Accept(decoded.Value.(*sdk.EvidencePackageChunk))
		if e != nil {
			break
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if rowErr != nil {
		return rowErr
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if complete != nil {
		if _, e = tx.Exec("INSERT OR IGNORE INTO evidence_receipts(connection,device,event_id,hash,path,created) VALUES(?,?,?,?,?,?)", row.connection, row.device, event, hash, complete.PackagePath, time.Now().Unix()); e != nil {
			return e
		}
		kind := "SNAPSHOT"
		if s.devices[row.connection+"/"+row.device].Format == "protobuf" {
			kind = "EVIDENCE_KIND_SNAPSHOT"
		}
		if e = s.scheduleJob(tx, row.connection, row.device, "evidence_ack", map[string]any{"eventId": event, "kind": kind, "packageSha256": hash}, stableKey(row.connection, row.device, event, "ack:"+hash), false); e != nil {
			return e
		}
		if _, e = tx.Exec("DELETE FROM evidence_chunks WHERE connection=? AND device=? AND event_id=? AND hash=?", row.connection, row.device, event, hash); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("UPDATE inbox SET status='done',error=NULL WHERE id=?", row.id); e != nil {
		return fmt.Errorf("commit image inbox: %w", e)
	}
	return tx.Commit()
}
