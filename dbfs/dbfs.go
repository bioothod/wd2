package dbfs

import (
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"fmt"
	"os"
	"time"
)

type DbFS struct {
	db		*sql.DB
	bp		*BucketProcessor
}

func NewDbFS(dbtype, dbparams string, e *EbucketCtl) (*DbFS, error) {
	db, err := sql.Open(dbtype, dbparams)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %s, params: %s: %v", dbtype, dbparams, err)
	}

	bp, err := NewBucketProcessor(e)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("could not create bucket processor: %v", err)
	}

	ctl := &DbFS {
		db:		db,
		bp:		bp,
	}

	return ctl, nil
}

func (ctl *DbFS) Close() {
	ctl.db.Close()
	ctl.bp.Close()
}

type DirEntry struct {
	Username		string
	Filename		string
	Parent			string
	Bucket			string
	Fmode			os.FileMode
	Fsize			uint64
	Created			time.Time
	Modified		time.Time
}

func (ent *DirEntry) String() string {
	return fmt.Sprintf("username: %s, filename: %s, parent: %s, bucket: %s, mode: %o, size: %d, created: '%s', modified: '%s'",
		ent.Username, ent.Filename, ent.Parent, ent.Bucket, ent.Fmode, ent.Fsize, ent.Created.String(), ent.Modified.String())
}

func (ctl *DbFS) InsertEntry(ent *DirEntry) error {
	ent.Created = time.Now()
	ent.Modified = ent.Created

	_, err := ctl.db.Exec("INSERT INTO dirs SET username=?,filename=?,parent=?,bucket=?,mode=?,size=?,created=?,modified=?",
		ent.Username, ent.Filename, ent.Parent, ent.Bucket, ent.Fmode, ent.Fsize, ent.Created, ent.Modified)
	if err != nil {
		return fmt.Errorf("could not insert new dir entry: %s: %v", ent.String(), err)
	}

	return nil
}

func (ctl *DbFS) DeleteEntry(ent *DirEntry) error {
	_, err := ctl.db.Exec("DELETE FROM dirs WHERE username=? AND filename=?", ent.Username, ent.Filename)
	if err != nil {
		return fmt.Errorf("could not delete dir entry: %s: %v", ent.String(), err)
	}

	return nil
}

func (ctl *DbFS) StatEntry(ent *DirEntry) error {
	rows, err := ctl.db.Query("SELECT * FROM dirs WHERE username=? AND filename=?", ent.Username, ent.Filename)
	if err != nil {
		return fmt.Errorf("could not read userinfo for user: %s: %v", ent.Username, err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, filename string

		err = rows.Scan(&username, &filename, &ent.Parent, &ent.Bucket, &ent.Fmode, &ent.Fsize, &ent.Created, &ent.Modified)
		if err != nil {
			return fmt.Errorf("database schema mismatch: %v", err)
		}

		return nil
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("could not scan database: %v", err)
	}

	return fmt.Errorf("there is no entry %s", ent.String())
}

func (ctl *DbFS) ScanEntryPrefix(ent *DirEntry) ([]*DirEntry, error) {
	rows, err := ctl.db.Query("SELECT * FROM dirs WHERE username=? AND parent=? AND filename like ?",
		ent.Username, ent.Parent, ent.Filename)
	if err != nil {
		return nil, fmt.Errorf("could not read userinfo for user: %s: %v", ent.Username, err)
	}
	defer rows.Close()

	entries := make([]*DirEntry, 0)
	for rows.Next() {
		var e DirEntry

		err = rows.Scan(&e.Username, &e.Filename, &e.Parent, &e.Bucket, &e.Fmode, &e.Fsize, &e.Created, &e.Modified)
		if err != nil {
			return nil, fmt.Errorf("database schema mismatch: %v", err)
		}

		entries = append(entries, &e)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("could not scan database: %v", err)
	}

	return entries, nil
}

func (ctl *DbFS) UpdateEntry(ent *DirEntry) error {
	_, err := ctl.db.Exec("UPDATE dirs SET mode=?,size=?,modified=?,bucket=? WHERE username=? AND filename=?",
		ent.Fmode, ent.Fsize, ent.Modified, ent.Bucket,
		ent.Username, ent.Filename)
	if err != nil {
		return fmt.Errorf("could not update entry: %s: %v", ent.String(), err)
	}

	return nil
}

func (ctl *DbFS) Ping() error {
	return ctl.db.Ping()
}
