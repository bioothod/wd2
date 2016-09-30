package dbfs

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/bioothod/ebucket-go"
	"github.com/golang/glog"
	"io"
	"time"
)

type EbucketCtl struct {
	LogFile		string			`json:"log_file"`
	LogLevel	string			`json:"log_level"`
	Remotes		[]string		`json:"remotes"`
	Mgroups		[]uint32		`json:"metadata_groups"`
	BucketKey	string			`json:"bucket_key"`
	Bnames		[]string		`json:"buckets"`
}

type BucketProcessor struct {
	node		*elliptics.Node
	bp		*ebucket.BucketProcessor
}

const RandomKeyLength = 128

func GenerateRandomKey(username string) (string, error) {
	b := make([]byte, RandomKeyLength)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return "", err
	}

	return username + ":" + base64.URLEncoding.EncodeToString(b), nil
}

func NewBucketProcessor (e *EbucketCtl) (*BucketProcessor, error) {
	node, err := elliptics.NewNode(e.LogFile, e.LogLevel)
	if err != nil {
		return nil, err
	}
	err = node.AddRemotes(e.Remotes)
	if err != nil {
		node.Free()
		return nil, err
	}

	var bp *ebucket.BucketProcessor
	if e.BucketKey != "" {
		bp, err = ebucket.NewBucketProcessorKey(node, e.Mgroups, e.BucketKey)
	} else {
		bp, err = ebucket.NewBucketProcessor(node, e.Mgroups, e.Bnames)
	}

	if err != nil {
		node.Free()
	}

	return &BucketProcessor {
		node:		node,
		bp:		bp,
	}, nil
}

func (bp *BucketProcessor) Close() {
	if bp != nil {
		bp.bp.Close()
		bp.node.Free()
	}
}

func (f *File) ReadDataFrom(r io.Reader) (int64, error) {
	bp := f.User.FS.bp
	if bp == nil {
		return 0, fmt.Errorf("read_from: bucket processor is not initialized")
	}

	if f.User.TotalSize == 0 {
		return io.Copy(f, r)
	}

	session, err := elliptics.NewSession(bp.node)
	if err != nil {
		return 0, fmt.Errorf("read_from: could not create new session, username: %s, filename: %s, error: %v",
			f.User.Username, f.Info.Filename, err)
	}
	defer session.Delete()

	var meta *ebucket.BucketMeta
	if f.Info.Bucket == "" {
		meta, err = bp.bp.GetBucket(uint64(f.User.TotalSize))
		if err != nil {
			return 0, fmt.Errorf("read_from: could not get bucket, username: %s, filename: %s, error: %v",
				f.User.Username, f.Info.Filename, err)
		}
		session.SetGroups(meta.Groups)
		session.SetNamespace(meta.Name)

		f.Info.Bucket = meta.Name

		f.Info.Key, err = GenerateRandomKey(f.User.Username)
		if err != nil {
			return 0, fmt.Errorf("read_from: could not generate new key, " +
				"bucket: %s, groups: %v, username: %s, filename: %s, error: %v",
				meta.Name, meta.Groups, f.User.Username, f.Info.Filename, err)
		}

	} else {
		meta, err = bp.bp.FindBucket(f.Info.Bucket)
		if err != nil {
			return 0, fmt.Errorf("read_from: could not find bucket: %s, username: %s, filename: %s, error: %v",
				f.Info.Bucket, f.User.Username, f.Info.Filename, err)
		}
		session.SetGroups(meta.Groups)
		session.SetNamespace(meta.Name)
	}

	var size uint64
	write_error := fmt.Errorf("write error: empty result from session.WriteData()")

	for ret := range session.WriteData(f.Info.Key, r, uint64(f.remote_offset), uint64(f.User.TotalSize)) {
		if ret.Error() != nil {
			glog.Errorf("read_from: username: %s, bucket: %s, groups: %v, key: %s, filename: %s, " +
				"remote_offset: %d, total_size: %d, write error: %v",
				f.User.Username, f.Info.Bucket, meta.Groups, f.Info.Key, f.Info.Filename,
				f.remote_offset, f.User.TotalSize, ret.Error())

			// do not return error if there was at least one successfull write
			// otherwise return the last error
			if write_error != nil {
				write_error = ret.Error()
			}
			continue
		}

		write_error = nil
		size = ret.Info().Size

		glog.Infof("read_from: username: %s, bucket: %s, groups: %v, key: %s, filename: %s, " +
			"remote_offset: %d, size: %d/%d",
			f.User.Username, f.Info.Bucket, meta.Groups, f.Info.Key, f.Info.Filename,
			f.remote_offset, size, f.User.TotalSize)
	}

	if write_error != nil {
		return 0, fmt.Errorf("read_from: username: %s, bucket: %s, groups: %v, key: %s, filename: %s, " +
				"remote_offset: %d, total_size: %d, write error: %v",
				f.User.Username, f.Info.Bucket, meta.Groups, f.Info.Key, f.Info.Filename,
				f.remote_offset, f.User.TotalSize, write_error)
	}

	f.remote_offset += int64(size)

	if uint64(f.remote_offset) > f.Info.Fsize {
		f.Info.Fsize = uint64(f.remote_offset)
	}
	f.Info.Modified = time.Now()

	err = f.User.FS.UpdateEntry(f.Info)
	if err != nil {
		return 0, fmt.Errorf("read_from: could not update dir entry: %s, error: %v", f.Info.String(), err)
	}

	return int64(size), nil
}

func (f *File) WriteData(p []byte) (int, error) {
	bp := f.User.FS.bp
	if bp == nil {
		return 0, fmt.Errorf("bucket processor is not initialized")
	}

	session, err := elliptics.NewSession(bp.node)
	if err != nil {
		return 0, fmt.Errorf("could not create new session, username: %s, filename: %s, error: %v",
			f.User.Username, f.Info.Filename, err)
	}
	defer session.Delete()

	var meta *ebucket.BucketMeta
	if f.Info.Bucket == "" {
		meta, err = bp.bp.GetBucket(uint64(len(p)))
		if err != nil {
			return 0, fmt.Errorf("could not get bucket, username: %s, filename: %s, remote_offset: %d, size: %d, error: %v",
				f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
		}
		session.SetGroups(meta.Groups)
		session.SetNamespace(meta.Name)

		f.Info.Bucket = meta.Name

		f.Info.Key, err = GenerateRandomKey(f.User.Username)
		if err != nil {
			return 0, fmt.Errorf("could not generate new key, bucket: %s, groups: %v, username: %s, filename: %s, " +
				"remote_offset: %d, size: %d, error: %v",
				meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
		}

	} else {
		meta, err = bp.bp.FindBucket(f.Info.Bucket)
		if err != nil {
			return 0, fmt.Errorf("could not find bucket: %s, username: %s, filename: %s, remote_offset: %d, size: %d, error: %v",
				f.Info.Bucket, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
		}
		session.SetGroups(meta.Groups)
		session.SetNamespace(meta.Name)
	}

	total_size := uint64(f.remote_offset) + uint64(len(p))
	writer, err := elliptics.NewWriteSeeker(session, f.Info.Key, f.remote_offset, total_size, 0)
	if err != nil {
		return 0, fmt.Errorf("could not create new writer, bucket: %s, key: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, f.Info.Key, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	defer writer.Free()

	copied, err := writer.Write(p)
	if err != nil {
		return 0, fmt.Errorf("could not write data, bucket: %s, key: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, f.Info.Key, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	if uint64(f.remote_offset) + uint64(len(p)) > f.Info.Fsize {
		f.Info.Fsize = uint64(f.remote_offset) + uint64(len(p))
	}
	f.Info.Modified = time.Now()

	err = f.User.FS.UpdateEntry(f.Info)
	if err != nil {
		return 0, fmt.Errorf("could not update dir entry, bucket: %s, key: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, f.Info.Key, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	f.remote_offset += int64(copied)

	return copied, nil
}

func (f *File) ReadData(p []byte) (int, error) {
	bp := f.User.FS.bp
	if bp == nil {
		return 0, fmt.Errorf("bucket processor is not initialized")
	}

	if f.Info.Bucket == "" {
		return 0, io.EOF
	}

	if uint64(f.remote_offset) >= f.Info.Fsize {
		return 0, io.EOF
	}

	session, err := elliptics.NewSession(bp.node)
	if err != nil {
		return 0, fmt.Errorf("could not create new session, username: %s, filename: %s, error: %v",
			f.User.Username, f.Info.Filename, err)
	}
	defer session.Delete()

	meta, err := bp.bp.FindBucket(f.Info.Bucket)
	if err != nil {
		return 0, fmt.Errorf("could not find bucket: %s, username: %s, filename: %s, remote_offset: %d, size: %d, error: %v",
			f.Info.Bucket, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	session.SetGroups(meta.Groups)
	session.SetNamespace(meta.Name)

	reader, err := elliptics.NewReadSeekerOffsetSize(session, f.Info.Key, uint64(f.remote_offset), uint64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("could not create new reader, bucket: %s, key: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, f.Info.Key, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	defer reader.Free()

	copied, err := reader.Read(p)
	if err != nil {
		return 0, fmt.Errorf("could not write data, bucket: %s, key: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, f.Info.Key, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	f.remote_offset += int64(copied)

	return copied, nil
}

func (f *File) RemoveData() error {
	bp := f.User.FS.bp
	if bp == nil {
		return fmt.Errorf("bucket processor is not initialized")
	}

	session, err := elliptics.NewSession(bp.node)
	if err != nil {
		return fmt.Errorf("could not create new session, username: %s, filename: %s, error: %v",
			f.User.Username, f.Info.Filename, err)
	}
	defer session.Delete()

	meta, err := bp.bp.FindBucket(f.Info.Bucket)
	if err != nil {
		return fmt.Errorf("could not find bucket: %s, username: %s, filename: %s, error: %v",
			f.Info.Bucket, f.User.Username, f.Info.Filename, err)
	}
	session.SetGroups(meta.Groups)
	session.SetNamespace(meta.Name)

	for ret := range session.Remove(f.Info.Key) {
		if ret.Error() == nil {
			return nil
		}

		err = ret.Error()
	}

	return err
}
