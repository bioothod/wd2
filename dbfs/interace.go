package dbfs

import (
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/webdav"
	"os"
	"path"
	"strings"
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
	fmt.Printf("rename: %s -> %s\n", oldName, newName)

	oent := NewDirEntry(ctl.Username, oldName)
	if oent.Filename == "/" {
		return os.ErrInvalid
	}
	nent := NewDirEntry(ctl.Username, newName)
	if nent.Filename == "/" {
		return os.ErrInvalid
	}

	if oent.Filename == nent.Filename {
		return nil
	}

	if strings.HasPrefix(nent.Filename, oent.Filename + "/") {
		// We can't rename oldName to be a sub-directory of itself.
		return os.ErrInvalid
	}

	// check whether src object exists
	err := ctl.FS.StatEntry(oent)
	if err != nil {
		return err
	}

	if oent.IsDir() {
		// if we are moving a directory, check whether destination path already exists, in this case it should be a directory
		err := ctl.FS.StatEntry(nent)
		fmt.Printf("nent: %s, err: %v\n", nent.String(), err)
		if err == nil {
			if !nent.IsDir() {
				return fmt.Errorf("move: %s -> %s: destination is not a directory", oent.Filename, nent.Filename)
			}

			nf := &File {
				User: ctl,
				Info: nent,
			}

			fi, err := nf.Readdir(0)
			if err != nil {
				return err
			}

			if len(fi) != 0 {
				return fmt.Errorf("move: %s -> %s: destination directory is not empty (%d entries)",
					oent.Filename, nent.Filename, len(fi))
			}
		}
	}

	err = ctl.FS.DeleteEntry(oent)
	if err != nil {
		return err
	}

	nent.Bucket = oent.Bucket
	nent.Fmode = oent.Fmode
	nent.Fsize = oent.Fsize

	err = ctl.FS.InsertEntry(nent)
	if err != nil {
		return err
	}

	if !oent.IsDir() {
		return nil
	}

	of := &File {
		User: ctl,
		Info: oent,
	}

	fi, err := of.Readdir(0)
	if err != nil {
		return err
	}

	for _, se := range fi {
		src := oent.Filename + "/" + se.Name()
		dst := nent.Filename + "/" + se.Name()

		err = ctl.Rename(src, dst)
		if err != nil {
			return err
		}
	}

	return nil
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
		return nil, os.ErrNotExist
		//return nil, fmt.Errorf("stat: %v", err)
	}

	return ent, nil
}
