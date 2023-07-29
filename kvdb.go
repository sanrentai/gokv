package gokv

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type KVDB struct {
	mu           sync.RWMutex
	memTable     map[string]string   // 内存表
	diskTables   []map[string]string // 磁盘表
	walFile      *os.File            // WAL文件
	dataFilePath string              // 数据文件路径
}

func NewKVDB(dataFilePath string) (*KVDB, error) {
	db := &KVDB{
		memTable:     make(map[string]string),
		diskTables:   make([]map[string]string, 0),
		dataFilePath: dataFilePath,
	}

	// 加载磁盘表
	err := db.loadDiskTables()
	if err != nil {
		return nil, err
	}

	// 打开WAL文件
	err = db.openWALFile()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// 打开WAL文件
func (db *KVDB) openWALFile() error {
	walFilePath := filepath.Join(db.dataFilePath, "wal.log")
	file, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	db.walFile = file
	return nil
}

// 关闭WAL文件
func (db *KVDB) closeWALFile() error {
	return db.walFile.Close()
}

// 将键值对插入数据库
func (db *KVDB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 写入WAL
	err := db.writeToWAL(WalRecord{Key: key, Value: value})
	if err != nil {
		return err
	}

	// 写入内存表
	db.memTable[key] = value

	// 判断内存表大小，如果达到阈值，则刷入磁盘表
	if len(db.memTable) >= 100 {
		db.flushMemTableToDisk()
	}

	return nil
}

// 通过键获取值
func (db *KVDB) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 先在内存表中查找
	value, ok := db.memTable[key]
	if ok {
		return value, nil
	}

	// 在磁盘表中查找
	for i := len(db.diskTables) - 1; i >= 0; i-- {
		value, ok = db.diskTables[i][key]
		if ok {
			return value, nil
		}
	}

	return "", fmt.Errorf("Key not found: %s", key)
}

// 将内存表数据刷入磁盘表
func (db *KVDB) flushMemTableToDisk() error {
	// 创建新的磁盘表
	diskTable := make(map[string]string)
	for k, v := range db.memTable {
		diskTable[k] = v
	}
	db.diskTables = append(db.diskTables, diskTable)

	// 清空内存表
	db.memTable = make(map[string]string)

	// 将磁盘表持久化到磁盘文件
	err := db.persistDiskTables()
	if err != nil {
		return err
	}

	return nil
}

// 将磁盘表数据持久化到磁盘文件
func (db *KVDB) persistDiskTables() error {
	// 创建临时文件
	tmpFilePath := filepath.Join(db.dataFilePath, "tmp.db")
	tmpFile, err := os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	// 写入磁盘表数据到临时文件
	for _, diskTable := range db.diskTables {
		data, err := json.Marshal(diskTable)
		if err != nil {
			return err
		}
		_, err = tmpFile.Write(data)
		if err != nil {
			return err
		}
	}

	// 关闭WAL文件
	err = db.closeWALFile()
	if err != nil {
		return err
	}

	// 关闭临时文件
	err = tmpFile.Close()
	if err != nil {
		return err
	}

	// 删除原数据文件
	err = os.Remove(filepath.Join(db.dataFilePath, "data.db"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// 将临时文件重命名为数据文件
	err = os.Rename(tmpFilePath, filepath.Join(db.dataFilePath, "data.db"))
	if err != nil {
		return err
	}

	// 重新打开WAL文件
	err = db.openWALFile()
	if err != nil {
		return err
	}

	return nil
}

// 加载磁盘表数据
func (db *KVDB) loadDiskTables() error {
	filePath := filepath.Join(db.dataFilePath, "data.db")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// 数据文件不存在，无需加载
		return nil
	}

	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	for len(file) > 0 {
		var diskTable map[string]string
		err := json.Unmarshal(file, &diskTable)
		if err != nil {
			return err
		}
		db.diskTables = append(db.diskTables, diskTable)

		// 跳过已读取的磁盘表数据
		file = file[len(file):]
	}

	return nil
}

// 写入WAL
func (db *KVDB) writeToWAL(record WalRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = db.walFile.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// 关闭数据库
func (db *KVDB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.flushMemTableToDisk()
	db.closeWALFile()
}

type WalRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
