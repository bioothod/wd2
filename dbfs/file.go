package dbfs

import (
	"fmt"
	"github.com/golang/glog"
	"io"
	"os"
	"path"
)

type File struct {
	User *DbFSUser
	Info *DirEntry

	remote_offset int64
}

func (f *File) Close() error {
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	glog.Infof("read: %v, data_size: %d", f.Info.String(), len(p))
	if f.Info.IsDir() {
		return 0, os.ErrInvalid
	}

	return f.ReadData(p)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	npos := f.remote_offset
	switch whence {
	case os.SEEK_SET:
		npos = offset
	case os.SEEK_CUR:
		npos += offset
	case os.SEEK_END:
		npos = int64(f.Info.Fsize) + offset
	default:
		npos = -1
	}
	if npos < 0 {
		return 0, os.ErrInvalid
	}

	f.remote_offset = npos

	return f.remote_offset, nil
}

func (f *File) Write(p []byte) (n int, err error) {
	glog.Infof("write: %v, data_size: %d", f.Info.String(), len(p))

	if f.Info.IsDir() {
		return 0, os.ErrInvalid
	}

	return f.WriteData(p)
}

func (f *File) ReadFrom(r io.Reader) (int64, error) {
	glog.Infof("read_from: %v", f.Info.String())

	if f.Info.IsDir() {
		return 0, os.ErrInvalid
	}

	return f.ReadDataFrom(r)
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	if !f.Info.IsDir() {
		return nil, os.ErrInvalid
	}

	ent := NewDirEntry(f.Info.Username, fmt.Sprintf("%s/%%", f.Info.Filename))

	fi, err := f.User.FS.ScanEntryPrefix(ent)
	if err != nil {
		glog.Errorf("readdir: %s, error: %v", ent.String(), err)
		return nil, err
	}
	glog.Infof("readdir: %s, entries: %d", ent.String(), len(fi))

	if f.remote_offset > int64(len(fi)) {
		return nil, io.EOF
	}

	o := int(f.remote_offset)
	lret := int(int64(len(fi)) - f.remote_offset)
	if count > 0 && lret > count {
		lret = count
	}

	f.remote_offset += int64(lret)
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
