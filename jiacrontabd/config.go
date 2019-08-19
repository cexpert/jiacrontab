package jiacrontabd

import (
	"jiacrontab/pkg/file"
	"jiacrontab/pkg/util"
	"net"
	"reflect"

	"github.com/iwannay/log"

	"gopkg.in/ini.v1"
)

const (
	appname = "jiacrontabd"
)

type Config struct {
	LogLevel            string `opt:"log_level"`
	VerboseJobLog       bool   `opt:"verbose_job_log"`
	ListenAddr          string `opt:"listen_addr"`
	AdminAddr           string `opt:"admin_addr"`
	LogPath             string `opt:"log_path"`
	AutoCleanTaskLog    bool   `opt:"auto_clean_task_log"`
	NodeName            string `opt:"node_name"`
	BoardcastAddr       string `opt:"boardcast_addr"`
	ClientAliveInterval int    `opt:"client_alive_interval"`
	CfgPath             string
	Debug               bool `opt:"debug"`
	iniFile             *ini.File
	DriverName          string `opt:"driver_name"`
	DSN                 string `opt:"dsn"`
}

// Resolve 解析配置文件
func (c *Config) Resolve() error {
	c.iniFile = c.loadConfig(c.CfgPath) // 加载ini文件

	val := reflect.ValueOf(c).Elem() // 反射中使用 Elem()方法获取指针对应的
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		opt := field.Tag.Get("opt")
		if opt == "" {
			continue
		}
		sec := c.iniFile.Section("jiacrontabd")

		if !sec.HasKey(opt) {
			continue
		}

		key := sec.Key(opt)
		switch field.Type.Kind() {
		case reflect.Bool:
			v, err := key.Bool()
			if err != nil {
				log.Errorf("cannot resolve ini field %s err(%v)", opt, err)
			}
			val.Field(i).SetBool(v) // 根据索引，返回索引对应的结构体字段的信息，通过Set方法修改值。
		case reflect.String:
			val.Field(i).SetString(key.String())
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
			if v, err := key.Int64(); err != nil {
				log.Errorf("cannot convert to int64 type (%s)", err)
			} else {
				val.Field(i).SetInt(v)
			}
		}
	}

	if c.BoardcastAddr == "" { // 如果配置文件广播地址未设置，设置默认值
		_, port, _ := net.SplitHostPort(c.ListenAddr)
		c.BoardcastAddr = util.InternalIP() + ":" + port
	}

	return nil
}

func NewConfig() *Config {
	return &Config{
		LogLevel:            "warn",
		VerboseJobLog:       true,
		ListenAddr:          "127.0.0.1:20001",
		AdminAddr:           "127.0.0.1:20003",
		LogPath:             "./logs",
		AutoCleanTaskLog:    true,
		NodeName:            util.GetHostname(),
		CfgPath:             "./jiacrontabd.ini",
		DriverName:          "sqlite3",
		DSN:                 "data/jiacrontabd.db",
		ClientAliveInterval: 30,
	}
}

// loadConfig 加载config文件
func (c *Config) loadConfig(path string) *ini.File {
	if !file.Exist(path) { // 如果文件不存在，则创建文件
		f, err := file.CreateFile(path)
		if err != nil {
			panic(err)
		}
		f.Close()
	}

	iniFile, err := ini.Load(path) // load ini文件
	if err != nil {
		panic(err)
	}
	return iniFile
}
