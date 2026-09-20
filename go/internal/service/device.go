package service

import "database/sql"

func (s *Service) storeLatest(tx *sql.Tx, row inboxItem, suffix, payload string) error { // suffix is selected by processObject, never supplied by SQL callers.
	_, e := tx.Exec("INSERT INTO latest(connection,device,"+suffix+",received_count) VALUES(?,?,?,1) ON CONFLICT(connection,device) DO UPDATE SET "+suffix+"=excluded."+suffix+",received_count=latest.received_count+1", row.connection, row.device, payload)
	return e
}
