// 监听程序运行资源使用情况
package pprof

import (
	"jiacrontab/pkg/file"
	"path/filepath"
	"runtime"
	"runtime/pprof" // pprof包来做代码的性能监控
	"time"

	"github.com/iwannay/log"
)

// ListenPprof 当通过系统kill命令发送SIGUSR1信号是，记录当前性能指标信息
func ListenPprof() {
	// 启动协程进行性能监听
	go listenSignal()
}

// cpuprofile 获取cpu性能指标
func cpuprofile() {
	path := filepath.Join("pprof", "cpuprofile")
	log.Debugf("profile save in %s", path)

	f, err := file.CreateFile(path)
	if err != nil {
		log.Error("could not create CPU profile: ", err)
		return
	}

	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Error("could not start CPU profile: ", err)
	} else {
		time.Sleep(time.Minute)
	}
	defer pprof.StopCPUProfile()
}

// memprofile 获取内存性能指标
func memprofile() {
	path := filepath.Join("pprof", "memprofile")
	log.Debugf("profile save in %s", path)
	f, err := file.CreateFile(path)
	if err != nil {
		log.Error("could not create memory profile: ", err)
		return
	}

	defer f.Close()

	runtime.GC() // get up-to-date statistics
	// 快速的获取堆栈信息，并写入文件中
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Error("could not write memory profile: ", err)
	}
}

// profile 获取其他系统指标
func profile() {
	names := []string{
		"goroutine",
		"heap",
		"allocs",
		"threadcreate",
		"block",
		"mutex",
	}
	for _, name := range names {
		path := filepath.Join("pprof", name)
		log.Debugf("profile save in %s", path)
		f, err := file.CreateFile(path)
		if err != nil {
			log.Error(err)
			continue
		}
		pprof.Lookup(name).WriteTo(f, 0)
	}

}
