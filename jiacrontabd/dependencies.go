package jiacrontabd

import (
	"bytes"
	"context"
	"jiacrontab/pkg/proto"
	"time"

	"github.com/iwannay/log"
)

// depEntry 依赖实例
type depEntry struct {
	jobID       uint            // 定时任务id
	jobUniqueID string          // job的唯一标志
	processID   int             // 当前依赖的父级任务（可能存在多个并发的task)
	id          string          // depID uuid
	once        bool            // 是否只执行一次
	workDir     string          // 工作目录
	user        string          // 用户
	env         []string        // 环境
	from        string          //
	commands    []string        // 命令
	dest        string          //
	done        bool            //
	timeout     int64           // 超时时间
	err         error           //
	ctx         context.Context // 上下文
	name        string          //
	logPath     string          //
	logContent  []byte          //
}

func newDependencies(jd *Jiacrontabd) *dependencies {
	return &dependencies{
		jd:  jd,
		dep: make(chan *depEntry, 100),
	}
}

type dependencies struct {
	jd  *Jiacrontabd
	dep chan *depEntry // depEntry的channel，单队列
}

// add 向 dependencies 队列实例中插入depEntry
func (d *dependencies) add(t *depEntry) {
	select {
	case d.dep <- t: // 如果 d.dep 为空，可以传入成功，否则丢弃
	default:
		log.Warnf("discard %v", t)
	}

}

func (d *dependencies) run() {
	go func() {
		for {
			select {
			case t := <-d.dep:
				go d.exec(t)
			}
		}
	}()
}

// TODO: 主任务退出杀死依赖
func (d *dependencies) exec(task *depEntry) {

	var (
		reply bool
		err   error
	)

	if task.timeout == 0 {
		// 默认超时10分钟
		task.timeout = 600
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.timeout)*time.Second)
	defer cancel()
	myCmdUnit := cmdUint{
		args:          [][]string{task.commands},
		ctx:           ctx,
		dir:           task.workDir,
		user:          task.user,
		logPath:       task.logPath,
		ignoreFileLog: true,
		jd:            d.jd,
		exportLog:     true,
	}

	log.Infof("dep start exec %s->%v", task.name, task.commands)
	task.err = myCmdUnit.launch()
	task.logContent = bytes.TrimRight(myCmdUnit.content, "\x00")
	task.done = true
	log.Infof("exec %s %s cost %.4fs %v", task.name, task.commands, float64(myCmdUnit.costTime)/1000000000, err)

	task.dest, task.from = task.from, task.dest

	if !d.jd.SetDependDone(task) {
		err = d.jd.rpcCallCtx(ctx, "Srv.SetDependDone", proto.DepJob{
			Name:        task.name,
			Dest:        task.dest,
			From:        task.from,
			ID:          task.id,
			JobUniqueID: task.jobUniqueID,
			ProcessID:   task.processID,
			JobID:       task.jobID,
			Commands:    task.commands,
			LogContent:  task.logContent,
			Err:         err,
			Timeout:     task.timeout,
		}, &reply)

		if err != nil {
			log.Error("Srv.SetDependDone error:", err, "server addr:", d.jd.getOpts().AdminAddr)
		}

		if !reply {
			log.Errorf("task %s %v call Srv.SetDependDone failed! err:%v", task.name, task.commands, err)
		}
	}
}
