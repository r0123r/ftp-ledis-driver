package ledisdriver

import (
	"os"
	"time"
)

type FileInfo struct {
	name    string
	isDir   bool
	modTime time.Time
	size    int64
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Size() int64 {
	return f.size
}

func (f *FileInfo) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | os.ModePerm
	}
	return os.ModePerm
}

func (f *FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *FileInfo) IsDir() bool {
	return f.isDir
}

func (f *FileInfo) Sys() interface{} {
	return nil
}

func (f *FileInfo) Owner() string {
	return "root"
}

func (f *FileInfo) Group() string {
	return "root"
}
