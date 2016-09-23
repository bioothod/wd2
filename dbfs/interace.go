package dbfs

import (
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/webdav"
	"os"
	"path"
	"time"
)

type DbFSUser struct {
	FS *DbFS
	Username string
}

var ErrNotSupported = fmt.Errorf("dbfs: operation not supported")

func NewDirEntry(username, filename string) *DirEntry {
	name := path.Clean(filename)
	parent_full := path.Clean(path.Dir(name))
	_, parent := path.Split(parent_full)

	if parent == "" {
		parent = "/"
	}

	return &DirEntry {
		Filename: name,
		Parent: parent,
		Username: username,
	}
}

func (ctl *DbFSUser) Mkdir(name string, perm os.FileMode) error {
	ent := NewDirEntry(ctl.Username, name)
	ent.Fmode = perm.Perm() | os.ModeDir

	err := ctl.FS.InsertEntry(ent)
	glog.Infof("mkdir: %s, error: %v", ent.String(), err)
	return err
}

func (ctl *DbFSUser) OpenFile(name string, flag int, perm os.FileMode) (webdav.File, error) {
	ent := NewDirEntry(ctl.Username, name)
	ent.Fmode = perm.Perm()

	err := ctl.FS.StatEntry(ent)
	if err != nil {
		return nil, fmt.Errorf("openfile: %v", err)
	}

	f := &File {
		User: ctl,
		Info: ent,
	}
	glog.Infof("openfile: %s, isdir: %t", ent.String(), ent.IsDir())

	return f, nil
}

func (ctl *DbFSUser) RemoveAll(name string) error {
	ent := NewDirEntry(ctl.Username, name)
	if ent.Filename == "/" {
		return os.ErrInvalid
	}

	return ctl.FS.DeleteEntry(ent)
}

func (ctl *DbFSUser) Rename(oldName, newName string) error {
	return ErrNotSupported
}

func (ent *DirEntry) Name() string {
	return ent.Filename
}
func (ent *DirEntry) Size() int64 {
	return int64(ent.Fsize)
}
func (ent *DirEntry) Mode() os.FileMode {
	return os.FileMode(ent.Fmode)
}
func (ent *DirEntry) ModTime() time.Time {
	return ent.Modified
}
func (ent *DirEntry) IsDir() bool {
	return ent.Mode().IsDir()
}
func (ent *DirEntry) Sys() interface{} {
	return nil
}

func (ctl *DbFSUser) Stat(name string) (os.FileInfo, error) {
	ent := NewDirEntry(ctl.Username, name)

	err := ctl.FS.StatEntry(ent)
	if err != nil {
		return nil, fmt.Errorf("stat: %v", err)
	}

	return ent, nil
}
