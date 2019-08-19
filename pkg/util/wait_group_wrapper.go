// 创建 WaitGroup，用来阻塞主协程退出
// 新增协程WaitGroup + 1，协程执行完成，WaitGroup - 1
package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup // sync.WaitGroup 能够一直等到所有的goroutine执行完成，并且阻塞主线程的执行，直到所有的goroutine执行完成
}

// Wrap 将传入函数加入到 WaitGroup
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1) // 等待组队列加1
	// 创建协程，执行对应的函数句柄
	go func() {
		cb()
		w.Done() // 函数执行完整，等待组减1
	}()
}
