package jiacrontabd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"jiacrontab/pkg/kproc"
	"jiacrontab/pkg/proto"
	"jiacrontab/pkg/util"
	"os"
	"runtime/debug"
	"time"

	"github.com/iwannay/log"
)

// cmdUint 命令执行单元
// args [['bash','test.sh'] ['grep','tset']]
type cmdUint struct {
	ctx              context.Context // 上下文
	args             [][]string      // 参数，二维数组
	logPath          string          // 日志路径
	content          []byte          // 命令内容，命令执行返回的内容，用于web加载
	logFile          *os.File        // 日志文件
	label            string          // 标签
	user             string          // 执行用户
	verboseLog       bool            // 是否输出详细日志
	exportLog        bool            // 是否输出日志
	ignoreFileLog    bool            // 忽略文件日志
	env              []string        // 环境变量
	killChildProcess bool            // 主进程退出时是否kill子进程,默认kill
	dir              string          // 目录
	startTime        time.Time       // 开始时间
	costTime         time.Duration   // 耗时
	jd               *Jiacrontabd    // Jiacrontabd实例指针
}

// release 在执行完成后调用，关闭文件句柄，计算执行耗时
func (cu *cmdUint) release() {
	if cu.logFile != nil {
		cu.logFile.Close()
	}
	cu.costTime = time.Now().Sub(cu.startTime)
}

// launch 开始运行时执行
func (cu *cmdUint) launch() error {
	defer func() {
		// recover主函数中出现的err
		if err := recover(); err != nil {
			log.Errorf("wrapExecScript error:%v\n%s", err, debug.Stack())
		}
		cu.release() // 执行函数扫尾
	}()
	cfg := cu.jd.getOpts()    // 获取配置文件信息
	cu.startTime = time.Now() // 开始时间

	var err error
	// 设置日志文件
	if err = cu.setLogFile(); err != nil {
		return err
	}

	if len(cu.args) > 1 {
		// 如果job的args数大于1个，运行命令
		err = cu.pipeExec()
	} else {
		err = cu.exec()
	}

	if err != nil {
		var errMsg string
		var prefix string
		if cu.verboseLog {
			prefix = fmt.Sprintf("[%s %s %s] ", time.Now().Format(proto.DefaultTimeLayout), cfg.BoardcastAddr, cu.label)
			errMsg = prefix + err.Error() + "\n"
		} else {
			prefix = fmt.Sprintf("[%s %s] ", cfg.BoardcastAddr, cu.label)
			errMsg = prefix + err.Error() + "\n"
		}

		cu.writeLog([]byte(errMsg)) // 记录err日志
		if cu.exportLog { // 如果设置日志范围为True
			cu.content = append(cu.content, []byte(errMsg)...) // 给web展示用
		}

		return err
	}

	return nil
}

// setLogFile 打开job的logPath
func (cu *cmdUint) setLogFile() error {
	var err error

	// 判断是否忽略日志输出
	if cu.ignoreFileLog {
		return nil
	}
	// 尝试打开文件
	// os.O_APPEND|os.O_CREATE|os.O_RDWR 是与运算
	cu.logFile, err = util.TryOpen(cu.logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR)
	if err != nil {
		return err
	}
	return nil
}

// writeLog 日志写入
func (cu *cmdUint) writeLog(b []byte) {
	if cu.ignoreFileLog {
		return
	}
	cu.logFile.Write(b)
}

// exec 执行操作
func (cu *cmdUint) exec() error {
	log.Debug("cmd exec args:", cu.args)
	if len(cu.args) == 0 { // 如果没有命令参数，返回err
		return errors.New("invalid args")
	}
	cu.args[0] = util.FilterEmptyEle(cu.args[0])          // 过滤空的参数
	cmdName := cu.args[0][0]                              // 取出命令名 /bin/bash test.sh 02 ，取出/bin/bash
	args := cu.args[0][1:]                                // 取出参数的切片
	cmd := kproc.CommandContext(cu.ctx, cmdName, args...) // 获取cmd的上下文对象
	cfg := cu.jd.getOpts()                                // 获取jiacrontabd的配置信息

	cmd.SetDir(cu.dir)                               // 设置cmd上下文对象的工作目录
	cmd.SetEnv(cu.env)                               // 设置环境变量
	cmd.SetUser(cu.user)                             // 设置执行用户
	cmd.SetExitKillChildProcess(cu.killChildProcess) // 设置主进程退出时是否kill子进程,默认kill

	stdout, err := cmd.StdoutPipe() // 获取cmd的标准输出对象
	if err != nil {
		return err
	}

	defer stdout.Close()

	stderr, err := cmd.StderrPipe() // 获取cmd的标准错误对象

	if err != nil {
		return err
	}

	defer stderr.Close()

	// 执行cmd  ##############################
	if err := cmd.Start(); err != nil {
		return err
	}

	reader := bufio.NewReader(stdout)    // 获取标准输出的reader
	readerErr := bufio.NewReader(stderr) // 获取标准错误的reader
	// 如果已经存在日志则直接写入
	cu.writeLog(cu.content)
	// 启动一个协程来 读取 命令执行的输出
	go func() {
		var (
			err  error
			line []byte
		)
		// 循环获取命令执行的标准输出
		for {

			line, err = reader.ReadBytes('\n') // 每次读入一行
			if err != nil || err == io.EOF {
				break
			}

			if cfg.VerboseJobLog {
				prefix := fmt.Sprintf("[%s %s %s] ", time.Now().Format(proto.DefaultTimeLayout), cfg.BoardcastAddr, cu.label)
				line = append([]byte(prefix), line...)
			}

			if cu.exportLog {
				cu.content = append(cu.content, line...)
			}
			cu.writeLog(line)
		}
		// 循环获取命令的标准错误
		for {
			line, err = readerErr.ReadBytes('\n') // 每次读入一行
			if err != nil || err == io.EOF {
				break
			}
			// 默认给err信息加上日期标志
			if cfg.VerboseJobLog {
				prefix := fmt.Sprintf("[%s %s %s] ", time.Now().Format(proto.DefaultTimeLayout), cfg.BoardcastAddr, cu.label)
				line = append([]byte(prefix), line...)
			}
			if cu.exportLog {
				cu.content = append(cu.content, line...)
			}
			cu.writeLog(line)
		}
	}()
	// 通过同步组，等待命令协程执行完成，阻塞主协程
	if err = cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// pipeExec 管道命令执行
func (cu *cmdUint) pipeExec() error {
	var (
		outBufer       bytes.Buffer
		errBufer       bytes.Buffer
		cmdEntryList   []*pipeCmd //多个命令实例切片，管道情况下使用
		err, exitError error
		line           []byte
		cfg            = cu.jd.getOpts()
	)
	// 遍历参数清单，
	for _, v := range cu.args {
		v = util.FilterEmptyEle(v) // 剔除空参数
		cmdName := v[0]
		args := v[1:]

		cmd := kproc.CommandContext(cu.ctx, cmdName, args...)

		cmd.SetDir(cu.dir)
		cmd.SetEnv(cu.env)
		cmd.SetUser(cu.user)
		cmd.SetExitKillChildProcess(cu.killChildProcess)

		cmdEntryList = append(cmdEntryList, &pipeCmd{cmd})
	}
	// 执行命令
	exitError = execute(&outBufer, &errBufer,
		cmdEntryList...,
	)

	// 如果已经存在日志则直接写入
	cu.writeLog(cu.content)

	for {

		line, err = outBufer.ReadBytes('\n')
		if err != nil || err == io.EOF {
			break
		}
		if cfg.VerboseJobLog {
			prefix := fmt.Sprintf("[%s %s %s] ", time.Now().Format(proto.DefaultTimeLayout), cfg.BoardcastAddr, cu.label)
			line = append([]byte(prefix), line...)
		}

		cu.content = append(cu.content, line...)
		cu.writeLog(line)
	}

	for {
		line, err = errBufer.ReadBytes('\n')
		if err != nil || err == io.EOF {
			break
		}

		if cfg.VerboseJobLog {
			prefix := fmt.Sprintf("[%s %s %s] ", time.Now().Format(proto.DefaultTimeLayout), cfg.BoardcastAddr, cu.label)
			line = append([]byte(prefix), line...)
		}

		if cu.exportLog {
			cu.content = append(cu.content, line...)
		}
		cu.writeLog(line)
	}
	return exitError
}

func call(stack []*pipeCmd, pipes []*io.PipeWriter) (err error) {
	if stack[0].Process == nil {
		if err = stack[0].Start(); err != nil {
			return err
		}
	}

	if len(stack) > 1 {
		if err = stack[1].Start(); err != nil {
			return err
		}

		defer func() {
			pipes[0].Close()
			if err == nil {
				err = call(stack[1:], pipes[1:])
			}
			if err != nil {
				// fixed zombie process
				stack[1].Wait()
			}
		}()
	}
	return stack[0].Wait()
}

type pipeCmd struct {
	*kproc.KCmd
}

func execute(outputBuffer *bytes.Buffer, errorBuffer *bytes.Buffer, stack ...*pipeCmd) (err error) {
	pipeStack := make([]*io.PipeWriter, len(stack)-1)
	i := 0
	for ; i < len(stack)-1; i++ {
		stdinPipe, stdoutPipe := io.Pipe()
		stack[i].Stdout = stdoutPipe
		stack[i].Stderr = errorBuffer
		stack[i+1].Stdin = stdinPipe
		pipeStack[i] = stdoutPipe
	}

	// 取出管道模块cmd的标准输出和标准错误
	stack[i].Stdout = outputBuffer
	stack[i].Stderr = errorBuffer

	if err = call(stack, pipeStack); err != nil {
		errorBuffer.WriteString(err.Error())
	}
	return err
}
