package jiacrontabd

import (
	"context"
	"encoding/json"
	"fmt"
	"jiacrontab/models"
	"jiacrontab/pkg/proto"
	"path/filepath"
	"sync"
	"time"

	"github.com/iwannay/log"
)

type ApiNotifyArgs struct {
	JobName        string
	JobID          uint
	NodeAddr       string
	CreateUsername string
	CreatedAt      time.Time
	NotifyType     string
}

type daemonJob struct {
	job        *models.DaemonJob  // 守护进程任务，数据库模型
	daemon     *Daemon            // 守护进程
	ctx        context.Context    // 上下文
	cancel     context.CancelFunc // 长下文取消句柄
	processNum int                //
}

// do 运行常驻进程
func (d *daemonJob) do(ctx context.Context) {

	d.processNum = 1                     // 任务号
	t := time.NewTicker(1 * time.Second) // 定时器
	defer t.Stop()

	d.daemon.wait.Add(1)         // 同步组加1
	cfg := d.daemon.jd.getOpts() // 获取jiacrontab实例的配置
	retryNum := d.job.RetryNum   // 任务重试次数

	defer func() { // recover 执行的错误
		if err := recover(); err != nil {
			log.Errorf("%s exec panic %s \n", d.job.Name, err)
		}
		d.processNum = 0
		// 更新job的执行状态为失败，并更新错误信息
		if err := models.DB().Model(d.job).Update("status", models.StatusJobStop).Error; err != nil {
			log.Error(err)
		}

		d.daemon.wait.Done()

	}()
	// 更新job对应的基础信息到DB
	if err := models.DB().Model(d.job).Updates(map[string]interface{}{
		"start_at": time.Now(),
		"status":   models.StatusJobRunning}).Error; err != nil {
		log.Error(err)
	}

	for {

		var (
			stop bool
			err  error
		)
		arg := d.job.Command // 获取job对应的命令
		if d.job.Code != "" {
			// 如果用户录入了代码，将代码追加到arg
			arg = append(arg, d.job.Code)
		}
		// 初始化cmdUint实例，这里是具体的cmd执行单元，从这里开始真正的执行任务
		myCmdUint := cmdUint{
			ctx:   ctx,
			args:  [][]string{arg}, // 这里的二维数组，只把arg赋值给[0][0]，数据放在了第二维的数组里
			env:   d.job.WorkEnv,
			dir:   d.job.WorkDir,
			user:  d.job.WorkUser,
			label: d.job.Name,
			jd:    d.daemon.jd,
			// 指定日志文件，通过job的信息生成路径信息
			logPath: filepath.Join(cfg.LogPath, "daemon_job", time.Now().Format("2006/01/02"), fmt.Sprintf("%d.log", d.job.ID)),
		}

		log.Info("exec daemon job, jobName:", d.job.Name, " jobID", d.job.ID)

		err = myCmdUint.launch()
		retryNum--
		d.handleNotify(err)

		select {
		case <-ctx.Done():
			stop = true
		case <-t.C:
		}

		if stop || d.job.FailRestart == false || (d.job.RetryNum > 0 && retryNum == 0) {
			break
		}

		if err = d.syncJob(); err != nil {
			break
		}

	}
	t.Stop()

	d.daemon.PopJob(d.job.ID)

	log.Info("daemon task end", d.job.Name)
}

func (d *daemonJob) syncJob() error {
	return models.DB().Take(d.job, "id=? and status=?", d.job.ID, models.StatusJobRunning).Error
}

func (d *daemonJob) handleNotify(err error) {
	if err == nil {
		return
	}

	var reply bool
	cfg := d.daemon.jd.getOpts()
	if d.job.ErrorMailNotify && len(d.job.MailTo) > 0 {
		var reply bool
		err := d.daemon.jd.rpcCallCtx(d.ctx, "Srv.SendMail", proto.SendMail{
			MailTo:  d.job.MailTo,
			Subject: cfg.BoardcastAddr + "提醒常驻脚本异常退出",
			Content: fmt.Sprintf(
				"任务名：%s<br/>创建者：%s<br/>开始时间：%s<br/>异常：%s",
				d.job.Name, d.job.CreatedUsername, time.Now().Format(proto.DefaultTimeLayout), err),
		}, &reply)
		if err != nil {
			log.Error("Srv.SendMail error:", err, "server addr:", cfg.AdminAddr)
		}
	}

	if d.job.ErrorAPINotify && len(d.job.APITo) > 0 {
		postData, err := json.Marshal(ApiNotifyArgs{
			JobName:        d.job.Name,
			JobID:          d.job.ID,
			CreateUsername: d.job.CreatedUsername,
			CreatedAt:      d.job.CreatedAt,
			NodeAddr:       cfg.BoardcastAddr,
			NotifyType:     "error",
		})
		if err != nil {
			log.Error("json.Marshal error:", err)
		}
		err = d.daemon.jd.rpcCallCtx(d.ctx, "Srv.ApiPost", proto.ApiPost{
			Urls: d.job.APITo,
			Data: string(postData),
		}, &reply)

		if err != nil {
			log.Error("Logic.ApiPost error:", err, "server addr:", cfg.AdminAddr)
		}
	}
}

// Daemon 守护进程结构体
type Daemon struct {
	taskChannel chan *daemonJob     // 常住任务chan队列
	taskMap     map[uint]*daemonJob // 常住任务job map
	jd          *Jiacrontabd        //
	lock        sync.Mutex          // 锁
	wait        sync.WaitGroup      // 同步组
}

// newDaemon 新建Daemon
func newDaemon(taskChannelLength int, jd *Jiacrontabd) *Daemon {
	return &Daemon{
		taskMap:     make(map[uint]*daemonJob),                // 初始化常住任务job map
		taskChannel: make(chan *daemonJob, taskChannelLength), // 初始化常住任务chan队列，指定channel长度
		jd:          jd,
	}
}

func (d *Daemon) add(t *daemonJob) {
	if t != nil {
		log.Debugf("daemon.add(%s)\n", t.job.Name)
		t.daemon = d
		d.taskChannel <- t
	}
}

// PopJob 删除调度列表中的任务
func (d *Daemon) PopJob(jobID uint) {
	d.lock.Lock()
	t := d.taskMap[jobID]
	if t != nil {
		delete(d.taskMap, jobID)
		d.lock.Unlock()
		t.cancel()
	} else {
		d.lock.Unlock()
	}
}

func (d *Daemon) run() {
	var jobList []models.DaemonJob
	err := models.DB().Where("status=?", models.StatusJobRunning).Find(&jobList).Error
	if err != nil {
		log.Error("init daemon task error:", err)
	}

	for _, v := range jobList {
		job := v
		d.add(&daemonJob{
			job: &job,
		})
	}

	d.process()
}

func (d *Daemon) process() {
	go func() {
		for v := range d.taskChannel {
			d.lock.Lock()
			if t := d.taskMap[v.job.ID]; t == nil {
				d.taskMap[v.job.ID] = v
				d.lock.Unlock()
				v.ctx, v.cancel = context.WithCancel(context.Background())
				go v.do(v.ctx)
			} else {
				d.lock.Unlock()
			}
		}
	}()
}

func (d *Daemon) count() int {
	var count int
	d.lock.Lock()
	count = len(d.taskMap)
	d.lock.Unlock()
	return count
}

func (d *Daemon) waitDone() {
	d.wait.Wait()
}
