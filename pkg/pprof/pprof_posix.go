// +build !windows

package pprof

import (
	"os"
	"os/signal" //实现对信号的处理
	"syscall"
)

func listenSignal() {
	// 创建一个队列长度为1的异步 channel，用于存放信号
	signChan := make(chan os.Signal, 1)
	// 第一个参数接收信号的 channel, 第二个及后面的参数表示设置要监听的信号，如果不设置表示监听所有的信号。
	signal.Notify(signChan, syscall.SIGUSR1) // 监控系统的 syscall.SIGUSR1 的信号
	// 如果有 syscall.SIGUSR1 信号触发， signChan 会存在数据
	for {
		<-signChan // signChan在没有数据是会被阻塞，接收到系统的 SIGUSR1 信号后，阻塞结束
		profile()
		memprofile()
		cpuprofile()
	}
}
