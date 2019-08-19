package jiacrontabd

import (
	"context"
	"jiacrontab/models"
	"jiacrontab/pkg/crontab"
	"jiacrontab/pkg/finder"
	"jiacrontab/pkg/proto"
	"jiacrontab/pkg/rpc"
	"sync/atomic"

	"github.com/iwannay/log"

	"jiacrontab/pkg/util"
	"sync"
	"time"

	"fmt"
)

// Jiacrontabd scheduling center
// 全局只有一个 Jiacrontabd 实例
type Jiacrontabd struct {
	crontab         *crontab.Crontab      // 存在优先级的定时任务队列，任务切片
	jobs            map[uint]*JobEntry    // job实例map
	tmpJobs         map[string]*JobEntry  // 临时job实例map，只执行一次的job
	dep             *dependencies         // 依赖job实体的channel队列
	daemon          *Daemon               // 常住任务队列
	heartbeatPeriod time.Duration         // 心跳间隔，默认5s
	mux             sync.RWMutex          // 读写锁
	startTime       time.Time             // jiacrontabd进程运行开始时间，用于计算进程运行时长
	cfg             atomic.Value          // jiacrontabd对应的配置信息
	wg              util.WaitGroupWrapper // sync.WaitGroup能够一直等到所有的goroutine执行完成，并且阻塞主线程的执行，直到所有的goroutine执行完成
}

// New return a Jiacrontabd instance
func New(opt *Config) *Jiacrontabd {
	j := &Jiacrontabd{
		jobs:    make(map[uint]*JobEntry), //
		tmpJobs: make(map[string]*JobEntry),

		heartbeatPeriod: 5 * time.Second,
		crontab:         crontab.New(),
	}
	j.swapOpts(opt)
	j.dep = newDependencies(j)
	j.daemon = newDaemon(100, j)

	return j
}

// 获取Jiacrontabd实例的cfg，config信息
func (j *Jiacrontabd) getOpts() *Config {
	// 类型断言，这里Load()返回的是一个 interface {} 接口
	// 接口.(类型) 意思是进行类型转换，即将接口转换成某种类型
	return j.cfg.Load().(*Config)
}

func (j *Jiacrontabd) swapOpts(opts *Config) {
	//
	j.cfg.Store(opts)
}

func (j *Jiacrontabd) addTmpJob(job *JobEntry) {
	j.mux.Lock()
	j.tmpJobs[job.uniqueID] = job
	j.mux.Unlock()
}

func (j *Jiacrontabd) removeTmpJob(job *JobEntry) {
	j.mux.Lock()
	delete(j.tmpJobs, job.uniqueID)
	j.mux.Unlock()
}

func (j *Jiacrontabd) addJob(job *crontab.Job, updateLastExecTime bool) {
	// 对Jiacrontabd 写入加锁
	j.mux.Lock()
	if v, ok := j.jobs[job.ID]; ok { // 如果job id在队列中，更新job
		v.job = job
	} else { // 如果job id不在队列中，新建一个job实体
		j.jobs[job.ID] = newJobEntry(job, j)
	}
	// 释放锁
	j.mux.Unlock()

	if err := j.crontab.AddJob(job); err != nil {
		log.Error("NextExecutionTime:", err, " timeArgs:", job)
	} else {
		data := map[string]interface{}{
			"next_exec_time": job.GetNextExecTime(),
			"status":         models.StatusJobTiming,
		}

		if updateLastExecTime {
			data["last_exec_time"] = time.Now()
		}

		if err := models.DB().Model(&models.CrontabJob{}).Where("id=?", job.ID).
			Updates(data).Error; err != nil {
			log.Error(err)
		}
	}
}

func (j *Jiacrontabd) execTask(job *crontab.Job) {

	j.mux.RLock()
	if task, ok := j.jobs[job.ID]; ok {
		j.mux.RUnlock()
		task.exec()
		return
	}
	log.Errorf("not found jobID(%d)", job.ID)
	j.mux.RUnlock()

}

func (j *Jiacrontabd) killTask(jobID uint) {
	var jobs []*JobEntry
	j.mux.RLock()
	if job, ok := j.jobs[jobID]; ok {
		jobs = append(jobs, job)
	}

	for _, v := range j.tmpJobs {
		if v.detail.ID == jobID {
			jobs = append(jobs, v)
		}
	}
	j.mux.RUnlock()

	for _, v := range jobs {
		v.kill()
	}
}

func (j *Jiacrontabd) run() {
	j.dep.run()
	j.daemon.run()
	j.wg.Wrap(j.crontab.QueueScanWorker)

	for v := range j.crontab.Ready() {
		v := v.Value.(*crontab.Job)
		j.execTask(v)
	}
}

// SetDependDone 依赖执行完毕时设置相关状态
// 目标网络不是本机时返回false
func (j *Jiacrontabd) SetDependDone(task *depEntry) bool {

	var (
		ok  bool
		job *JobEntry
	)

	if task.dest != j.getOpts().BoardcastAddr {
		return false
	}

	isAllDone := true

	j.mux.Lock()
	if job, ok = j.tmpJobs[task.jobUniqueID]; !ok {
		job, ok = j.jobs[task.jobID]
	}
	j.mux.Unlock()
	if ok {

		var logContent []byte
		var curTaskEntry *process

		for _, p := range job.processes {
			if int(p.id) == task.processID {
				curTaskEntry = p
				for _, dep := range p.deps {

					if dep.id == task.id {
						dep.dest = task.dest
						dep.from = task.from
						dep.logContent = task.logContent
						dep.err = task.err
						dep.done = true
					}

					if dep.done == false {
						isAllDone = false
					} else {
						logContent = append(logContent, dep.logContent...)
					}
					// 同步模式上一个依赖结束才会触发下一个
					if dep.id == task.id && task.err == nil && p.jobEntry.detail.IsSync {
						if err := j.dispatchDependSync(p.ctx, p.deps, dep.id); err != nil {
							task.err = err
						}
					}

				}
			}
		}

		if curTaskEntry == nil {
			log.Infof("cannot find task entry %s %s", task.name, task.commands)
			return true
		}

		// 如果依赖任务执行出错直接通知主任务停止
		if task.err != nil {
			isAllDone = true
			curTaskEntry.err = task.err
			log.Infof("depend %s %s exec failed, %s, try to stop master task", task.name, task.commands, task.err)
		}

		if isAllDone {
			curTaskEntry.ready <- struct{}{}
			curTaskEntry.jobEntry.logContent = append(curTaskEntry.jobEntry.logContent, logContent...)
		}

	} else {
		log.Infof("cannot find task handler %s %s", task.name, task.commands)
	}

	return true

}

// 同步模式根据depEntryID确定位置实现任务的依次调度
func (j *Jiacrontabd) dispatchDependSync(ctx context.Context, deps []*depEntry, depEntryID string) error {
	flag := true
	cfg := j.getOpts()
	if len(deps) > 0 {
		flag = false
		for _, v := range deps {
			// 根据flag实现调度下一个依赖任务
			if flag || depEntryID == "" {
				// 检测目标服务器为本机时直接执行脚本
				if v.dest == cfg.BoardcastAddr {
					j.dep.add(v)
				} else {
					var reply bool
					err := j.rpcCallCtx(ctx, "Srv.ExecDepend", []proto.DepJob{{
						ID:          v.id,
						Name:        v.name,
						Dest:        v.dest,
						From:        v.from,
						JobUniqueID: v.jobUniqueID,
						JobID:       v.jobID,
						ProcessID:   v.processID,
						Commands:    v.commands,
						Timeout:     v.timeout,
					}}, &reply)
					if !reply || err != nil {
						return fmt.Errorf("Srv.ExecDepend error:%v server addr:%s", err, cfg.AdminAddr)
					}
				}
				break
			}

			if v.id == depEntryID {
				flag = true
			}

		}
	}
	return nil
}

func (j *Jiacrontabd) dispatchDependAsync(ctx context.Context, deps []*depEntry) error {
	var depJobs proto.DepJobs
	cfg := j.getOpts()
	for _, v := range deps {
		// 检测目标服务器是本机直接执行脚本
		if v.dest == cfg.BoardcastAddr {
			j.dep.add(v)
		} else {
			depJobs = append(depJobs, proto.DepJob{
				ID:          v.id,
				Name:        v.name,
				Dest:        v.dest,
				From:        v.from,
				ProcessID:   v.processID,
				JobID:       v.jobID,
				JobUniqueID: v.jobUniqueID,
				Commands:    v.commands,
				Timeout:     v.timeout,
			})
		}
	}

	if len(depJobs) > 0 {
		var reply bool
		if err := j.rpcCallCtx(ctx, "Srv.ExecDepend", depJobs, &reply); err != nil {
			return fmt.Errorf("Srv.ExecDepend error:%v server addr: %s", err, cfg.AdminAddr)

		}
	}
	return nil
}

func (j *Jiacrontabd) count() int {
	j.mux.RLock()
	num := len(j.jobs)
	j.mux.RUnlock()
	return num
}

func (j *Jiacrontabd) deleteJob(jobID uint) {
	j.mux.Lock()
	delete(j.jobs, jobID)
	j.mux.Unlock()
}

func (j *Jiacrontabd) heartBeat() {
	var (
		reply    bool
		cronJobs []struct {
			Total   uint
			GroupID uint
			Failed  bool
			Status  models.JobStatus
		}
		daemonJobs []struct {
			Total   uint
			GroupID uint
			Status  models.JobStatus
		}
		ok             bool
		nodes          = make(map[uint]models.Node) // 初始化 nodes 字典
		cfg            = j.getOpts()
		nodeName       = cfg.NodeName // 节点主机名
		node           models.Node
		superGroupNode models.Node
	)

	if nodeName == "" { // 如果没有设置主机名，获取主机名
		nodeName = util.GetHostname() // 获取主机名
	}

	models.DB().Model(&models.CrontabJob{}).Select("id,group_id,status,failed,count(1) as total").Group("group_id,status,failed").Scan(&cronJobs)
	models.DB().Model(&models.DaemonJob{}).Select("id,group_id,status,count(1) as total").Group("group_id,status").Scan(&daemonJobs)

	nodes[models.SuperGroup.ID] = models.Node{
		Addr:    cfg.BoardcastAddr,
		GroupID: models.SuperGroup.ID,
		Name:    nodeName,
	}

	for _, job := range cronJobs {
		superGroupNode = nodes[models.SuperGroup.ID]
		if node, ok = nodes[job.GroupID]; !ok {
			node = models.Node{
				Addr:    cfg.BoardcastAddr,
				GroupID: job.GroupID,
				Name:    nodeName,
			}
		}

		if job.Failed && job.Status == models.StatusJobTiming || job.Status == models.StatusJobRunning {
			node.CrontabJobFailNum += job.Total
			superGroupNode.CrontabJobFailNum += job.Total
		}

		if job.Status == models.StatusJobUnaudited {
			node.CrontabJobAuditNum += job.Total
			superGroupNode.CrontabJobAuditNum += job.Total
		}

		if job.Status == models.StatusJobTiming || job.Status == models.StatusJobRunning {
			node.CrontabTaskNum += job.Total
			superGroupNode.CrontabTaskNum += job.Total
		}

		nodes[job.GroupID] = node
		nodes[models.SuperGroup.ID] = superGroupNode
	}

	for _, job := range daemonJobs {
		superGroupNode = nodes[models.SuperGroup.ID]
		if node, ok = nodes[job.GroupID]; !ok {
			node = models.Node{
				Addr:    cfg.BoardcastAddr,
				GroupID: job.GroupID,
				Name:    nodeName,
			}
		}
		if job.Status == models.StatusJobUnaudited {
			node.DaemonJobAuditNum += job.Total
			superGroupNode.DaemonJobAuditNum += job.Total
		}
		if job.Status == models.StatusJobRunning {
			node.DaemonTaskNum += job.Total
			superGroupNode.DaemonTaskNum += job.Total
		}
		nodes[job.GroupID] = node
		nodes[models.SuperGroup.ID] = superGroupNode
	}

	err := j.rpcCallCtx(context.TODO(), rpc.RegisterService, nodes, &reply)

	if err != nil {
		log.Error("Srv.Register error:", err, ",server addr:", cfg.AdminAddr)
	}

	time.AfterFunc(time.Duration(j.getOpts().ClientAliveInterval)*time.Second, j.heartBeat)
}

func (j *Jiacrontabd) recovery() {
	var crontabJobs []models.CrontabJob
	var daemonJobs []models.DaemonJob
	// db中搜索出job状态为 定时中 和 执行中， 结果存入crontabJobs
	err := models.DB().Find(&crontabJobs, "status IN (?)", []models.JobStatus{models.StatusJobTiming, models.StatusJobRunning}).Error
	if err != nil {
		log.Debug("crontab recovery:", err)
	}
	//
	for _, v := range crontabJobs {
		j.addJob(&crontab.Job{
			ID:      v.ID,
			Second:  v.TimeArgs.Second,
			Minute:  v.TimeArgs.Minute,
			Hour:    v.TimeArgs.Hour,
			Day:     v.TimeArgs.Day,
			Month:   v.TimeArgs.Month,
			Weekday: v.TimeArgs.Weekday,
		}, false)
	}

	err = models.DB().Find(&daemonJobs, "status in (?)", []models.JobStatus{models.StatusJobOk}).Error

	if err != nil {
		log.Debug("daemon recovery:", err)
	}

	for _, v := range daemonJobs {
		job := v
		j.daemon.add(&daemonJob{
			job: &job,
		})
	}

}

func (j *Jiacrontabd) init() {
	cfg := j.getOpts() // 获取Config实例
	// 初始化DB，并建立数据库连接
	if err := models.CreateDB(cfg.DriverName, cfg.DSN); err != nil {
		panic(err)
	}
	// 通过 models.DB() 获取db实例
	// AutoMigrate ORM 迁移，将定义的结构体迁移至数据库
	models.DB().AutoMigrate(&models.CrontabJob{}, &models.DaemonJob{})
	j.startTime = time.Now()
	// 判断是否需要清理任务日志
	if cfg.AutoCleanTaskLog {
		// 开启一个协程查询并删除任务日志
		// 清楚30天前的日志
		go finder.SearchAndDeleteFileOnDisk(cfg.LogPath, 24*time.Hour*30, 1<<30)
	}
	// 恢复数据库中状态为'定时中'和'执行中'的任务，加入job队列
	j.recovery()
}

func (j *Jiacrontabd) rpcCallCtx(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	return rpc.CallCtx(j.getOpts().AdminAddr, serviceMethod, ctx, args, reply)
}

// Main main function
func (j *Jiacrontabd) Main() {
	j.init() // Jiacrontabd实例初始化操作
	j.heartBeat()
	go j.run()
	// newCrontabJobSrv	新建CrontabJob实例
	// newDaemonJobSrv 新建DaemonJob实例
	// newSrv
	rpc.ListenAndServe(j.getOpts().ListenAddr, newCrontabJobSrv(j), newDaemonJobSrv(j), newSrv(j))
}
