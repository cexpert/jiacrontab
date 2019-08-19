// +build !windows

package kproc

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"

	"github.com/iwannay/log"
)

// CommandContext 重写原CommandContext，实现可以kill的Cmd(KCmd)
func CommandContext(ctx context.Context, name string, arg ...string) *KCmd {
	cmd := exec.CommandContext(ctx, name, arg...)
	cmd.SysProcAttr = &syscall.SysProcAttr{} // 操作系统特定进程的属性，设置该值也许会导致你的程序在某些操作系统上无法运行或者编译
	cmd.SysProcAttr.Setsid = true            // 实现守护进程，防止假死
	return &KCmd{
		ctx:                ctx,
		Cmd:                cmd,
		isKillChildProcess: true,                // 是否可以kill
		done:               make(chan struct{}), // 同步channel
	}
}

// SetUser 设置执行用户,要保证root权限
// Golang如何让子进程以另一个用户身份运行，jiacron用户为root，脚本执行用户为其他用户时
// 说明教程： https://www.jianshu.com/p/91ed708701f3
func (k *KCmd) SetUser(username string) {
	if username == "" {
		return
	}
	u, err := user.Lookup(username) // 查找系统中对应的用户
	if err != nil {
		log.Error("setUser error:", err)
		return
	}

	log.Infof("KCmd set uid=%s,gid=%s", u.Uid, u.Gid)
	k.SysProcAttr = &syscall.SysProcAttr{}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	// 执行进程运行的用户和组
	k.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}

}

func (k *KCmd) KillAll() {

	select {
	case k.done <- struct{}{}: // 空结构体写入 done channel，如果chan有数据，会被阻塞
	default:
	}

	if k.Process == nil {
		return
	}

	if k.isKillChildProcess == false {
		return
	}

	group, err := os.FindProcess(-k.Process.Pid)
	if err == nil {
		group.Signal(syscall.SIGKILL)
	}
}

// Wait
func (k *KCmd) Wait() error {
	defer k.KillAll()
	// 启动协程
	go func() {
		select {
		// Done 方法在Context被取消或超时时返回一个close的channel,close的channel可以作为广播通知，告诉给context相关的函数要停止当前工作然后返回。
		case <-k.ctx.Done(): // 主要只为了方便用户主动结束命令执行，上下文被结束时，ctx.Done()对应的channel可读出数据，阻塞结束，则执行killAll操作
			k.KillAll()
		case <-k.done: // 正常执行完成，k.done可读出数据，正常结束
		}
	}()
	return k.Cmd.Wait()
}
