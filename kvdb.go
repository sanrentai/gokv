package gokv

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

type KVDB struct {
	mu       sync.RWMutex
	Data     map[string]string
	dataFile string // 数据文件路径
}

func NewKVDB(dataFile string) (*KVDB, error) {
	db := &KVDB{
		Data:     make(map[string]string),
		dataFile: dataFile,
	}
	// 加载文件
	err := db.loadData()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// 将键值对插入数据库
func (db *KVDB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 写入内存表
	db.Data[key] = value
	return db.flush()
}

// 通过键获取值
func (db *KVDB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 查找
	value, ok := db.Data[key]
	return value, ok
}

// 删除
func (db *KVDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	delete(db.Data, key)
	return db.flush()
}

// 写入磁盘
func (db *KVDB) flush() error {
	data, err := json.Marshal(db.Data)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(db.dataFile, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

// 加载磁盘表数据
func (db *KVDB) loadData() error {
	if _, err := os.Stat(db.dataFile); os.IsNotExist(err) {
		// 数据文件不存在，无需加载
		return nil
	}

	data, err := ioutil.ReadFile(db.dataFile)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &db.Data)
}

// 关闭数据库
func (db *KVDB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.flush()
}
