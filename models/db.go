package models

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jinzhu/gorm"
)

// D alias DB
type D = gorm.DB

var (
	db        *D
	debugMode bool
)

func CreateDB(dialect string, args ...interface{}) error {
	switch dialect { // 判断数据库类型，进行不同的数据库连接操作
	case "sqlite3":
		return createSqlite3(dialect, args...)
	case "postgres", "mysql":
		var err error
		db, err = gorm.Open(dialect, args...) // 创建mysql数据库连接
		return err
	}
	return fmt.Errorf("unknow database type %s", dialect)
}

// createSqlite3 创建sqlite3文件，并建立db connect
func createSqlite3(dialect string, args ...interface{}) error {
	var err error
	if args[0] == nil {
		return errors.New("sqlite3:db file cannot empty")
	}

	dbDir := filepath.Dir(filepath.Clean(fmt.Sprint(args[0])))
	err = os.MkdirAll(dbDir, 0755)
	if err != nil {
		return fmt.Errorf("sqlite3:%s", err)
	}
	// db 数据模块级别的全局变量
	db, err = gorm.Open(dialect, args...) // 创建sqlite3数据库连接
	if err == nil {
		db.DB().SetMaxOpenConns(1) // 设置最大连接数
	}
	return err
}

// 外部模块调用db，需要通过DB()来获取
func DB() *D {
	if db == nil {
		panic("you must call CreateDb first")
	}

	if debugMode {
		return db.Debug()
	}
	return db
}

func Transactions(fn func(tx *gorm.DB) error) error {
	if fn == nil {
		return errors.New("fn is nil")
	}
	tx := DB().Begin()
	defer func() {
		if err := recover(); err != nil {
			DB().Rollback()
		}
	}()

	if fn(tx) != nil {
		tx.Rollback()
	}
	return tx.Commit().Error
}

// InitModel 初始化数据库，进行数据库迁移
func InitModel(driverName string, dsn string, debug bool) error {
	if driverName == "" || dsn == "" {
		return errors.New("driverName and dsn cannot empty")
	}
	// 创建数据库连接
	if err := CreateDB(driverName, dsn); err != nil {
		return err
	}

	debugMode = debug
	// 初始化Node,Group,User,Event,JobHistory
	DB().AutoMigrate(&Node{}, &Group{}, &User{}, &Event{}, &JobHistory{})
	/*
		这里需要做初始化判断，否则会出现下面的冲突的错误
		UNIQUE constraint failed: groups.id
	*/
	DB().Create(&SuperGroup) // 初始化supergroup
	return nil
}
