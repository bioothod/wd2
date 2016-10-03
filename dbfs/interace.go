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
	TotalSize int64
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

func (ctl *DbFSUser) OpenFile(name string, flags int, perm os.FileMode) (webdav.File, error) {
	ent := NewDirEntry(ctl.Username, name)
	ent.Fmode = perm

	flags_array := make([]string, 0)
	if (flags & os.O_CREATE) != 0 {
		flags_array = append(flags_array, "create")
	}
	if (flags & os.O_TRUNC) != 0 {
		flags_array = append(flags_array, "truncate")
	}
	if (flags & os.O_RDWR) != 0 {
		flags_array = append(flags_array, "rdwr")
	}
	if (flags & os.O_WRONLY) != 0 {
		flags_array = append(flags_array, "wronly")
	} else {
		flags_array = append(flags_array, "rdonly")
	}
	if (flags & os.O_APPEND) != 0 {
		flags_array = append(flags_array, "append")
	}
	glog.Infof("openfile: username: %s, filename: %s, flags: %x %v, perm: %s", ctl.Username, name, flags, flags_array, perm.String())

	if ent.Filename == "/" {
		if flags & (os.O_WRONLY | os.O_RDWR) != 0 {
				return nil, os.ErrPermission
		}
	}

	err := ctl.FS.StatEntry(ent)
	if err != nil {
		if (flags & os.O_CREATE) != 0 {
			err := ctl.FS.InsertEntry(ent)
			if err != nil {
				return nil, fmt.Errorf("openfile: could not create empty file: %v", err)
			}
			glog.Infof("openfile: username: %s, filename: %s, flags: %x %v, perm: %s: created new file",
				ctl.Username, name, flags, flags_array, perm.String())
		} else {
			glog.Errorf("openfile: could not stat file: %v", err)
			return nil, os.ErrNotExist
		}
	}

	// truncate
	if (flags & (os.O_WRONLY | os.O_RDWR) != 0) && (flags & os.O_TRUNC != 0) && (ent.Size() != 0) {
		ent.Fsize = 0
		err := ctl.FS.UpdateEntry(ent)
		if err != nil {
			return nil, fmt.Errorf("openfile: truncate failed: %v", err)
		}

		glog.Infof("openfile: username: %s, filename: %s, flags: %x %v, perm: %s: updated entry: %s",
			ctl.Username, name, flags, flags_array, perm.String(), ent.String())
	}

	f := &File {
		User: ctl,
		Info: ent,
	}

	return f, nil
}

func (ctl *DbFSUser) RemoveAll(name string) error {
	glog.Infof("remove: username: %s, filename: %s", ctl.Username, name)
	ent := NewDirEntry(ctl.Username, name)
	if ent.Filename == "/" {
		return os.ErrInvalid
	}

	err := ctl.FS.StatEntry(ent)
	if err != nil {
		glog.Errorf("remove: username: %s, filename: %s: there is no directory entry: %s", ctl.Username, name, err)
		return err
	}

	err = ctl.FS.DeleteEntry(ent)
	if err != nil {
		glog.Errorf("remove: %s: could not delete entry: %s", ent.String(), err)
		return err
	}

	glog.Infof("remove: %s: entry deleted", ent.String())

	if ent.IsDir() {
		return nil
	}

	if ent.Bucket != "" {
		f := &File {
			User: ctl,
			Info: ent,
		}

		err = f.RemoveData()
		if err != nil {
			glog.Errorf("remove: %s: could not remove data from elliptics: %v", ent.String(), err)
			return err
		}

		glog.Infof("remove: %s: entry deleted from elliptics", ent.String())
	}

	return nil
}

func (ctl *DbFSUser) Rename(oldName, newName string) error {
	glog.Infof("rename: username: %s, filename: %s -> %s", ctl.Username, oldName, newName)
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
		glog.Errorf("rename: username: %s, filename: %s -> %s: there is no old directory entry: %s",
			ctl.Username, oldName, newName, err)
		return err
	}

	if oent.IsDir() {
		// if we are moving a directory, check whether destination path already exists, in this case it should be a directory
		err := ctl.FS.StatEntry(nent)
		if err == nil {
			if !nent.IsDir() {
				glog.Errorf("rename: %s -> %s: destination is not a directory", oent.String(), nent.String())
				return fmt.Errorf("rename: %s -> %s: destination is not a directory", oent.Filename, nent.Filename)
			}

			nf := &File {
				User: ctl,
				Info: nent,
			}

			fi, err := nf.Readdir(0)
			if err != nil {
				glog.Errorf("rename: %s -> %s: src readdir failed: %v", oent.String(), nent.String(), err)
				return err
			}

			if len(fi) != 0 {
				glog.Errorf("rename: %s -> %s: destination directory is not empty (%d entries)",
					oent.String(), nent.String(), len(fi))

				return fmt.Errorf("rename: %s -> %s: destination directory is not empty (%d entries)",
					oent.Filename, nent.Filename, len(fi))
			}
		}
	}

	err = ctl.FS.DeleteEntry(oent)
	if err != nil {
		glog.Errorf("rename: %s -> %s: could not delete old entry: %v", oent.String(), nent.String(), err)
		return err
	}

	nfilename := nent.Filename
	nparent := nent.Parent

	nent = oent
	nent.Filename = nfilename
	nent.Parent = nparent

	err = ctl.FS.InsertEntry(nent)
	if err != nil {
		glog.Errorf("rename: %s -> %s: could not insert new entry: %v", oent.String(), nent.String(), err)
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
		glog.Errorf("rename: %s -> %s: dst readdir failed: %v", oent.String(), nent.String(), err)
		return err
	}

	for _, se := range fi {
		src := oent.Filename + "/" + se.Name()
		dst := nent.Filename + "/" + se.Name()

		err = ctl.Rename(src, dst)
		if err != nil {
			glog.Errorf("rename: %s -> %s: recursive rename failed: %s -> %s, error: %v",
				oent.String(), nent.String(), src, dst, err)
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
		glog.Errorf("stat: username: %s, filename: %s, error: %v", ent.Username, ent.Filename, err)
		return nil, os.ErrNotExist
		//return nil, fmt.Errorf("stat: %v", err)
	}

	glog.Infof("stat: %s", ent.String())
	return ent, nil
}
