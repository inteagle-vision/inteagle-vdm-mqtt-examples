package service

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Only acknowledged packages are eligible. Failed/pending work and orphaned
// partial files remain available for recovery, even when their age exceeds TTL.
func (s *Service) pruneEvidence(cutoff int64) error {
	s.evidenceMu.Lock()
	defer s.evidenceMu.Unlock()
	rows, e := s.db.Query("SELECT connection,device,event_id,hash,path FROM evidence_receipts WHERE created<?", cutoff)
	if e != nil {
		return e
	}
	type receipt struct{ connection, device, event, hash, path string }
	receipts := []receipt{}
	for rows.Next() {
		var r receipt
		if e = rows.Scan(&r.connection, &r.device, &r.event, &r.hash, &r.path); e != nil {
			rows.Close()
			return e
		}
		receipts = append(receipts, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, r := range receipts {
		var status string
		key := stableKey(r.connection, r.device, r.event, "ack:"+r.hash)
		if e = s.db.QueryRow("SELECT status FROM jobs WHERE key=?", key).Scan(&status); errors.Is(e, sql.ErrNoRows) {
			continue
		} else if e != nil {
			return e
		}
		if status != "done" {
			continue
		}
		if e = os.Remove(r.path); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		// One event directory can contain multiple hashes; do not remove another
		// package's receipt when expiring a previously acknowledged version.
		receiptPath := filepath.Join(filepath.Dir(r.path), "receipt.json")
		raw, readErr := os.ReadFile(receiptPath)
		if readErr == nil {
			var content map[string]string
			if e = json.Unmarshal(raw, &content); e != nil {
				return e
			}
			if content["packageSha256"] == r.hash {
				if e = os.Remove(receiptPath); e != nil && !errors.Is(e, os.ErrNotExist) {
					return e
				}
			}
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		if _, e = s.db.Exec("DELETE FROM evidence_receipts WHERE connection=? AND device=? AND event_id=? AND hash=?", r.connection, r.device, r.event, r.hash); e != nil {
			return e
		}
	}
	root := filepath.Join(s.dir, "evidence")
	return filepath.Walk(root, func(path string, info os.FileInfo, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		// JPEG files have no application ACK lifecycle. Never prune .part/.tmp.
		if info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".jpg") && filepath.Base(filepath.Dir(path)) == "jpeg" && info.ModTime().Unix() < cutoff {
			return os.Remove(path)
		}
		return nil
	})
}
