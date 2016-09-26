package dbfs

import (
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"github.com/bioothod/ebucket-go"
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
	bp.bp.Close()
	bp.node.Free()
}

func (f *File) WriteData(p []byte) (int, error) {
	bp := f.User.FS.bp
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
	} else {
		meta, err = bp.bp.FindBucket(f.Info.Bucket)
		if err != nil {
			return 0, fmt.Errorf("could not get bucket, username: %s, filename: %s, remote_offset: %d, size: %d, error: %v",
				f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
		}
		session.SetGroups(meta.Groups)
		session.SetNamespace(meta.Name)
	}

	key := fmt.Sprintf("%s:%s", f.User.Username, f.Info.Filename)
	writer, err := elliptics.NewWriteSeeker(session, key, f.remote_offset, uint64(len(p)), 0)
	if err != nil {
		return 0, fmt.Errorf("could not create new writer, bucket: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	defer writer.Free()

	copied, err := writer.Write(p)
	if err != nil {
		return 0, fmt.Errorf("could not write data, bucket: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	if uint64(f.remote_offset) + uint64(len(p)) > f.Info.Fsize {
		f.Info.Fsize = uint64(f.remote_offset) + uint64(len(p))
	}
	f.Info.Modified = time.Now()
	f.Info.Bucket = meta.Name

	err = f.User.FS.UpdateEntry(f.Info)
	if err != nil {
		return 0, fmt.Errorf("could not update dir entry, bucket: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	f.remote_offset += int64(copied)

	return copied, nil
}

func (f *File) ReadData(p []byte) (int, error) {
	if f.Info.Bucket == "" {
		return 0, io.EOF
	}

	if uint64(f.remote_offset) >= f.Info.Fsize {
		return 0, io.EOF
	}

	bp := f.User.FS.bp
	session, err := elliptics.NewSession(bp.node)
	if err != nil {
		return 0, fmt.Errorf("could not create new session, username: %s, filename: %s, error: %v",
			f.User.Username, f.Info.Filename, err)
	}
	defer session.Delete()

	meta, err := bp.bp.FindBucket(f.Info.Bucket)
	if err != nil {
		return 0, fmt.Errorf("could not get bucket, username: %s, filename: %s, remote_offset: %d, size: %d, error: %v",
			f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	session.SetGroups(meta.Groups)
	session.SetNamespace(meta.Name)

	key := fmt.Sprintf("%s:%s", f.User.Username, f.Info.Filename)
	reader, err := elliptics.NewReadSeekerOffsetSize(session, key, uint64(f.remote_offset), uint64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("could not create new writer, bucket: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}
	defer reader.Free()

	copied, err := reader.Read(p)
	if err != nil {
		return 0, fmt.Errorf("could not write data, bucket: %s, groups: %v, username: %s, filename: %s, " +
			"remote_offset: %d, size: %d, error: %v",
			meta.Name, meta.Groups, f.User.Username, f.Info.Filename, f.remote_offset, len(p), err)
	}

	f.remote_offset += int64(copied)

	return copied, nil
}
