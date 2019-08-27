package admin

import (
	"jiacrontab/models"
	"jiacrontab/pkg/mailer"
	"jiacrontab/pkg/rpc"

	"sync/atomic"

	"github.com/kataras/iris"
)

// Admin 结构
type Admin struct {
	cfg           atomic.Value //
	initAdminUser int32
}

// getOpts 获取admin实例中的cfg配置
func (adm *Admin) getOpts() *Config {
	return adm.cfg.Load().(*Config) // 类型断言
}

// swapOpts 存储config配置到admin实例中
func (adm *Admin) swapOpts(opts *Config) {
	adm.cfg.Store(opts) // 转储config
}

// New Admin构造方法
func New(opt *Config) *Admin {
	adm := &Admin{}
	adm.swapOpts(opt) // 将config实例转出到adm实例的 atomic.Value 中
	return adm
}

// init Admin实例初始化
func (adm *Admin) init() {
	cfg := adm.getOpts() // 获取admin的配置文件
	// 初始化数据库连接，初始化数据库，初始化supergroup
	if err := models.InitModel(cfg.Database.DriverName, cfg.Database.DSN, cfg.App.Debug); err != nil {
		panic(err)
	}
	// 查询group id 为1的组是否存在
	if models.DB().Take(&models.User{}, "group_id=?", 1).Error == nil {
		atomic.StoreInt32(&adm.initAdminUser, 1)
	}

	// mail
	if cfg.Mailer.Enabled {
		mailer.InitMailer(&mailer.Mailer{
			QueueLength:    cfg.Mailer.QueueLength,
			SubjectPrefix:  cfg.Mailer.SubjectPrefix,
			From:           cfg.Mailer.From,
			Host:           cfg.Mailer.Host,
			User:           cfg.Mailer.User,
			Passwd:         cfg.Mailer.Passwd,
			FromEmail:      cfg.Mailer.FromEmail,
			DisableHelo:    cfg.Mailer.DisableHelo,
			HeloHostname:   cfg.Mailer.HeloHostname,
			SkipVerify:     cfg.Mailer.SkipVerify,
			UseCertificate: cfg.Mailer.UseCertificate,
			CertFile:       cfg.Mailer.CertFile,
			KeyFile:        cfg.Mailer.KeyFile,
			UsePlainText:   cfg.Mailer.UsePlainText,
			HookMode:       false,
		})
	}
}

func (adm *Admin) Main() {

	cfg := adm.getOpts() // 获取config实例
	adm.init()           // 初始化，主要是数据库相关的初始化
	defer models.DB().Close()
	// 启动RPC服务监听
	go rpc.ListenAndServe(cfg.App.RPCListenAddr, NewSrv(adm))

	app := newApp(adm)
	app.Run(iris.Addr(cfg.App.HTTPListenAddr))
}
