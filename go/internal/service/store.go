package service

import (
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"syscall"
)

var ErrOverloaded = errors.New("durable inbox or evidence capacity exhausted")

func openStore(dir string) (*sql.DB, *os.File, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, nil, e
	}
	lock, e := os.OpenFile(filepath.Join(dir, "service.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, nil, errors.New("data directory already used by another service")
	}
	db, e := sql.Open("sqlite", filepath.Join(dir, "service.sqlite"))
	if e != nil {
		lock.Close()
		return nil, nil, e
	}
	db.SetMaxOpenConns(1)
	fail := func(e error) (*sql.DB, *os.File, error) { db.Close(); lock.Close(); return nil, nil, e }
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA busy_timeout=5000"} {
		if _, e = db.Exec(q); e != nil {
			return fail(e)
		}
	}
	var version int
	if e = db.QueryRow("PRAGMA user_version").Scan(&version); e != nil {
		return fail(e)
	}
	if version > 1 {
		return fail(fmt.Errorf("unsupported schema version %d", version))
	}
	if version == 0 {
		tx, e := db.Begin()
		if e != nil {
			return fail(e)
		}
		if _, e = tx.Exec(schema); e != nil {
			tx.Rollback()
			return fail(e)
		}
		if e = tx.Commit(); e != nil {
			return fail(e)
		}
	}
	return db, lock, nil
}

const schema = `
CREATE TABLE inbox(id INTEGER PRIMARY KEY AUTOINCREMENT, connection TEXT NOT NULL, device TEXT NOT NULL, topic TEXT NOT NULL, payload BLOB NOT NULL, created INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,error TEXT);
CREATE INDEX inbox_work ON inbox(status,next_at,id);
CREATE TABLE latest(connection TEXT NOT NULL,device TEXT NOT NULL,telemetry TEXT,attributes TEXT,received_count INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(connection,device));
CREATE TABLE alarm_events(connection TEXT NOT NULL,device TEXT NOT NULL,event_id TEXT NOT NULL,alarm_id TEXT NOT NULL,ts INTEGER NOT NULL,payload TEXT NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(connection,device,event_id));
CREATE TABLE alarms(connection TEXT NOT NULL,device TEXT NOT NULL,alarm_id TEXT NOT NULL,ts INTEGER NOT NULL,payload TEXT NOT NULL,needs_reconcile INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(connection,device,alarm_id));
CREATE TABLE outbox(key TEXT PRIMARY KEY,connection TEXT NOT NULL,device TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL,error TEXT);
CREATE TABLE jobs(key TEXT PRIMARY KEY,connection TEXT NOT NULL,device TEXT NOT NULL,kind TEXT NOT NULL,params TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL,error TEXT);
CREATE TABLE evidence_events(key TEXT PRIMARY KEY,connection TEXT NOT NULL,device TEXT NOT NULL,payload TEXT NOT NULL,created INTEGER NOT NULL);
CREATE TABLE evidence_chunks(connection TEXT NOT NULL,device TEXT NOT NULL,event_id TEXT NOT NULL,hash TEXT NOT NULL,chunk_index INTEGER NOT NULL,payload BLOB NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(connection,device,event_id,hash,chunk_index));
CREATE TABLE evidence_receipts(connection TEXT NOT NULL,device TEXT NOT NULL,event_id TEXT NOT NULL,hash TEXT NOT NULL,path TEXT NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(connection,device,event_id,hash));
PRAGMA user_version=1;`
