package crontab

import (
	"container/heap"
	"errors"
	"jiacrontab/pkg/pqueue"
	"sync"
	"time"
)

// Task 任务
type Task = pqueue.Item

// Crontab 一个存在优先级的定时任务队列
type Crontab struct {
	pq    pqueue.PriorityQueue // 优先队列，是一个任务Task切片
	mux   sync.RWMutex         // 读写锁
	ready chan *Task           // Task对应的channel
}

// New 新建Crontab
func New() *Crontab {
	return &Crontab{
		pq:    pqueue.New(10000),       // 初始化一个1W容量的优先级队列切片
		ready: make(chan *Task, 10000), // 初始化一个1W容量的任务channel
	}
}

// AddJob 添加未经处理的job
func (c *Crontab) AddJob(j *Job) error {
	nt, err := j.NextExecutionTime(time.Now())
	if err != nil {
		return errors.New("Invalid execution time")
	}
	c.mux.Lock()
	heap.Push(&c.pq, &Task{
		Priority: nt.UnixNano(),
		Value:    j,
	})
	c.mux.Unlock()
	return nil
}

// AddJob 添加延时任务
func (c *Crontab) AddTask(t *Task) {
	c.mux.Lock()
	heap.Push(&c.pq, t)
	c.mux.Unlock()
}

func (c *Crontab) Len() int {
	c.mux.RLock()
	len := len(c.pq)
	c.mux.RUnlock()
	return len
}

func (c *Crontab) GetAllTask() []*Task {
	c.mux.Lock()
	list := c.pq
	c.mux.Unlock()
	return list
}

func (c *Crontab) Ready() <-chan *Task {
	return c.ready
}

func (c *Crontab) QueueScanWorker() {
	refreshTicker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-refreshTicker.C:
			if len(c.pq) == 0 {
				continue
			}
		start:
			c.mux.Lock()
			now := time.Now().UnixNano()
			job, _ := c.pq.PeekAndShift(now)
			c.mux.Unlock()
			if job == nil {
				continue
			}
			c.ready <- job
			goto start

		}
	}
}
