package ledisdriver

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/r0123r/ftp-server"
	"github.com/r0123r/vredis/ledis"
)

type LedisDriver struct {
	db       *ledis.DB
	RootPath string
	server.Perm
}

func (driver *LedisDriver) Init(conn *server.Conn) {
	//driver.conn = conn
}

// params  - a file path
// returns - a time indicating when the requested path was last modified
//         - an error if the file doesn't exist or the user lacks
//           permissions
func (driver *LedisDriver) Stat(path string) (server.FileInfo, error) {

	if path == "/" {
		return &FileInfo{name: path, isDir: true}, nil
	}
	isdir := false
	rpath := driver.realPath(path)
	ok, err := driver.db.Exists(rpath)
	if err != nil {
		return nil, err
	}
	if ok != 1 {
		if !strings.HasSuffix(path, "/") {
			rpath = append(rpath, '/')
		}
		ok, _ := driver.db.HKeyExists(rpath)
		if ok != 1 {
			return nil, fmt.Errorf("Not exists:%q", rpath)
		}
		isdir = true
	}
	buf, _ := driver.db.HGet(rpath, []byte("modTime"))
	i, _ := ledis.Int64(buf, nil)
	modTime := time.Unix(i, 0)
	size := int64(0)
	ind := 0
	if !isdir {
		size, _ = driver.db.StrLen(rpath)
		ind = strings.LastIndex(path, "/")
		ind++
	} else {
		path = strings.TrimRight(path, "/")
	}

	return &FileInfo{path[ind:], isdir, modTime, size}, nil
}

// params  - path
// returns - true if the current user is permitted to change to the
//           requested path
func (driver *LedisDriver) ChangeDir(path string) error {
	if path != "/" {
		rpath := driver.realPath(path + "/")
		if ok, _ := driver.db.HKeyExists(rpath); ok != 1 {
			return fmt.Errorf("Not a directory")
		}
	}
	driver.RootPath = path
	return nil
}

// params  - path, function on file or subdir found
// returns - error
//           path
func (driver *LedisDriver) ListDir(prefix string, callback func(server.FileInfo) error) error {
	d := string(driver.realPath(prefix))
	cursor := []byte{}
	entries := [][]byte{}
	var f server.FileInfo
	var err error
	//Список каталогов
	for {
		dirs, _ := driver.db.Scan(ledis.HASH, cursor, 1, false, "^"+d+".*/$")
		if len(dirs) == 0 {
			break
		}
		sentry := string(dirs[0])
		cursor = dirs[0]
		key := strings.Trim(strings.TrimLeft(sentry, d), "/")
		if key == "" || strings.Contains(key, "/") {
			continue
		}
		f, err = driver.Stat(sentry) //&FileInfo{name: key, isDir: true}
		if err != nil {
			return err
		}
		if err = callback(f); err != nil {
			return err
		}
	}

	cursor = []byte{}
	//Список файлов
	for {
		ents, err := driver.db.Scan(ledis.KV, cursor, 100, false, "^"+d+".*[^/]")
		if err != nil {
			return err
		}
		entries = append(entries, ents...)
		if len(ents) < 100 {
			break
		}
		cursor = ents[len(ents)-1]
	}

	for _, entry := range entries {
		sentry := string(entry)
		key := strings.Trim(strings.TrimLeft(sentry, d), "/")
		if strings.Contains(key, "/") {
			continue
		}
		f, err = driver.Stat(sentry)
		if err != nil {
			return err
		}
		if err = callback(f); err != nil {
			return err
		}
	}

	return nil
}

// params  - path
// returns - nil if the directory was deleted or any error encountered
func (driver *LedisDriver) DeleteDir(path string) error {
	rpath := driver.realPath(path)
	for {
		ents, err := driver.db.Scan(ledis.HASH, []byte{}, 10, false, "^"+string(rpath)+"/.*")
		if err != nil {
			return err
		}
		driver.db.Del(ents...)
		driver.db.HMclear(ents...)
		if len(ents) < 10 {
			break
		}
	}
	return nil
}

// params  - path
// returns - nil if the file was deleted or any error encountered
func (driver *LedisDriver) DeleteFile(path string) error {
	rpath := driver.realPath(path)
	_, err := driver.db.Del(rpath)
	_, err = driver.db.HClear(rpath)
	return err
}

// params  - from_path, to_path
// returns - nil if the file was renamed or any error encountered
func (driver *LedisDriver) Rename(p1 string, p2 string) error {
	rp1 := driver.realPath(p1)
	rp2 := driver.realPath(p2)
	buf, err := driver.db.Get(rp1)
	if err != nil {
		return err
	}
	err = driver.db.Set(rp2, buf)
	if err != nil {
		return err
	}
	val, err := driver.db.HGetAll(rp1)
	if err != nil {
		return err
	}
	err = driver.db.HMset(rp2, val...)
	if err != nil {
		return err
	}
	_, err = driver.db.HClear(rp1)
	_, err = driver.db.Del(rp1)
	return err
}

// params  - path
// returns - nil if the new directory was created or any error encountered
func (driver *LedisDriver) MakeDir(path string) error {
	rpath := driver.realPath(path + "/")
	if ok, _ := driver.db.HKeyExists(rpath); ok != 1 {
		driver.db.HSet(rpath, []byte("modTime"), ledis.PutInt64(time.Now().Unix()))
	}
	return nil
}

func (driver *LedisDriver) realPath(path string) []byte {
	return []byte(strings.TrimLeft(path, "/"))
}

// params  - path
// returns - a string containing the file data to send to the client
func (driver *LedisDriver) GetFile(key string, offset int64) (int64, io.ReadCloser, error) {
	rpath := driver.realPath(key)
	buf, err := driver.db.GetRange(rpath, int(offset), -1)
	if err != nil {
		return 0, nil, err
	}

	return int64(len(buf)), NewSkipReadCloser(buf), nil
}

// params  - destination path, an io.Reader containing the file data
// returns - the number of bytes writen and the first error encountered while writing, if any.
func (driver *LedisDriver) PutFile(destPath string, data io.Reader, appendData bool) (int64, error) {
	rPath := driver.realPath(destPath)
	var isExist bool
	f, _ := driver.db.Exists(rPath)
	isExist = (f == 1)

	if !isExist || !appendData {
		driver.db.Set(rPath, []byte(""))
	}

	buf, err := ioutil.ReadAll(data)
	if err != nil {
		driver.db.Del(rPath)
		return 0, err
	}
	size, err := driver.db.Append(rPath, buf)
	if err != nil {
		driver.db.Del(rPath)
		return 0, err
	}

	driver.db.HSet(rPath, []byte("modTime"), ledis.PutInt64(time.Now().Unix()))
	return size, nil
}

type LedisDriverFactory struct {
	Ldb      *ledis.Ledis
	RootPath string
	server.Perm
}

func (factory *LedisDriverFactory) NewDriver() (server.Driver, error) {
	//	cfg := config.NewConfigDefault()
	//	cfg.DataDir = "var_ftp"
	//	ldb, err := ledis.Open(cfg)
	//	if err != nil {
	//		return nil, err
	//	}
	db, err := factory.Ldb.Select(0)
	if err != nil {
		return nil, err
	}

	return &LedisDriver{db, factory.RootPath, factory.Perm}, nil
}
