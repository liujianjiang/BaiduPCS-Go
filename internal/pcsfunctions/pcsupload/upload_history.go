package pcsupload

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/qjfoidnh/BaiduPCS-Go/internal/pcsconfig"
	"github.com/qjfoidnh/BaiduPCS-Go/pcsutil/checksum"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	// UploadHistoryDBName 上传历史记录数据库目录名
	UploadHistoryDBName = "pcs_upload_history"
)

type (
	// UploadedRecord 已上传文件的记录
	UploadedRecord struct {
		Path    string `json:"path"`    // 本地绝对路径
		Length  int64  `json:"length"`  // 文件大小
		MD5     []byte `json:"md5"`     // 文件MD5
		ModTime int64  `json:"modtime"` // 本地修改时间
	}

	// UploadHistory 上传历史记录数据库
	UploadHistory struct {
		lock   sync.RWMutex
		db     *leveldb.DB
		closed bool
	}
)

// NewUploadHistory 初始化上传历史记录数据库
func NewUploadHistory() (*UploadHistory, error) {
	dbPath := filepath.Join(pcsconfig.GetConfigDir(), UploadHistoryDBName)

	// 打开LevelDB
	opts := &opt.Options{
		NoSync: true, // 异步写入，提高性能
	}

	db, err := leveldb.OpenFile(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("打开上传历史数据库失败: %s", err)
	}

	return &UploadHistory{
		db: db,
	}, nil
}

// HasUploaded 检查文件是否已上传过且未修改
// 返回 true 表示文件已上传过且未修改，可以跳过
func (uh *UploadHistory) HasUploaded(meta *checksum.LocalFileMeta) bool {
	if uh == nil || uh.db == nil || meta == nil {
		return false
	}

	uh.lock.RLock()
	defer uh.lock.RUnlock()

	meta.CompleteAbsPath()

	// 先获取记录
	record, err := uh.getRecord(meta.Path)
	if err != nil || record == nil {
		return false
	}

	// 比较文件大小和修改时间
	if record.Length != meta.Length {
		return false
	}

	if record.ModTime != meta.ModTime {
		return false
	}

	return true
}

// Add 添加已上传文件记录
func (uh *UploadHistory) Add(meta *checksum.LocalFileMeta) error {
	if uh == nil || uh.db == nil || meta == nil {
		return fmt.Errorf("数据库未初始化")
	}

	uh.lock.Lock()
	defer uh.lock.Unlock()

	meta.CompleteAbsPath()

	record := &UploadedRecord{
		Path:    meta.Path,
		Length:  meta.Length,
		MD5:     meta.MD5,
		ModTime: meta.ModTime,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("序列化记录失败: %s", err)
	}

	// 使用文件路径作为key
	return uh.db.Put([]byte(meta.Path), data, nil)
}

// getRecord 获取指定路径的记录
func (uh *UploadHistory) getRecord(path string) (*UploadedRecord, error) {
	data, err := uh.db.Get([]byte(path), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	var record UploadedRecord
	err = json.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// Delete 删除指定路径的记录
func (uh *UploadHistory) Delete(path string) error {
	if uh == nil || uh.db == nil {
		return fmt.Errorf("数据库未初始化")
	}

	uh.lock.Lock()
	defer uh.lock.Unlock()

	return uh.db.Delete([]byte(path), nil)
}

// Clear 清理已不存在的文件记录
func (uh *UploadHistory) Clear() error {
	if uh == nil || uh.db == nil {
		return fmt.Errorf("数据库未初始化")
	}

	uh.lock.Lock()
	defer uh.lock.Unlock()

	iter := uh.db.NewIterator(nil, nil)
	defer iter.Release()

	var pathsToDelete [][]byte

	for iter.Next() {
		key := iter.Key()
		var record UploadedRecord
		err := json.Unmarshal(iter.Value(), &record)
		if err != nil {
			continue
		}

		// 检查文件是否还存在
		_, err = os.Stat(record.Path)
		if os.IsNotExist(err) {
			pathsToDelete = append(pathsToDelete, make([]byte, len(key)))
			copy(pathsToDelete[len(pathsToDelete)-1], key)
		}
	}

	// 删除不存在的记录
	for _, path := range pathsToDelete {
		err := uh.db.Delete(path, nil)
		if err != nil {
			pcsUploadVerbose.Warnf("清理历史记录失败: %s, err: %s\n", string(path), err)
		}
	}

	return nil
}

// Close 关闭数据库
func (uh *UploadHistory) Close() error {
	if uh == nil || uh.db == nil || uh.closed {
		return nil
	}

	uh.lock.Lock()
	defer uh.lock.Unlock()

	uh.closed = true
	return uh.db.Close()
}

// GetStats 获取数据库统计信息
func (uh *UploadHistory) GetStats() (count int, err error) {
	if uh == nil || uh.db == nil {
		return 0, fmt.Errorf("数据库未初始化")
	}

	uh.lock.RLock()
	defer uh.lock.RUnlock()

	iter := uh.db.NewIterator(nil, nil)
	defer iter.Release()

	count = 0
	for iter.Next() {
		count++
	}

	return count, nil
}
