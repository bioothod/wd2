package dbfs

import (
	"fmt"
	//"github.com/bioothod/elliptics-go/elliptics"
	"github.com/golang/glog"
	"io"
	"os"
	"path"
)

type File struct {
	User *DbFSUser
	Info *DirEntry

	readdir_offset int
}

func (f *File) Close() error {
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	return 0, ErrNotSupported
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return 0, ErrNotSupported
}

func (f *File) Write(p []byte) (n int, err error) {
	return 0, ErrNotSupported
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	ent := NewDirEntry(f.Info.Username, fmt.Sprintf("%s/%%", f.Info.Filename))

	fi, err := f.User.FS.ScanEntryPrefix(ent)
	if err != nil {
		glog.Errorf("readdir: %s, error: %v", ent.String(), err)
		return nil, err
	}
	glog.Infof("readdir: %s, entries: %d", ent.String(), len(fi))

	if f.readdir_offset > len(fi) {
		return nil, io.EOF
	}

	o := f.readdir_offset
	lret := len(fi) - f.readdir_offset
	if count > 0 && lret > count {
		lret = count
	}

	f.readdir_offset += lret
	ret := make([]os.FileInfo, 0, lret)
	for i := 0; i < lret; i++ {
		_, file := path.Split(fi[o + i].Filename)
		fi[o + i].Filename = file
		ret = append(ret, fi[o + i])
	}
	return ret, nil
}

func (f *File) Stat() (os.FileInfo, error) {
	return f.User.Stat(f.Info.Filename)
}
