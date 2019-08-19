package rpc

import (
	"github.com/iwannay/log"
	"net"
	"net/rpc"
)

// listen Start rpc server
func listen(addr string, srcvr ...interface{}) error {
	var err error
	for _, v := range srcvr {
		// 在DefaultServer中注册发布接收方的方法。
		if err = rpc.Register(v); err != nil {
			return err
		}
	}

	l, err := net.Listen("tcp4", addr) // tcp监听
	if err != nil {
		return err
	}
	defer func() {
		log.Info("listen rpc", addr, "close")
		if err := l.Close(); err != nil {
			log.Infof("listen.Close() error(%v)", err)
		}
	}()
	rpc.Accept(l) // 接受Listener上的连接，并为每个传入的连接向DefaultServer提供请求。
	return nil
}

// ListenAndServe  run rpc server
func ListenAndServe(addr string, srcvr ...interface{}) {
	log.Info("rpc server listen:", addr)
	err := listen(addr, srcvr...)
	if err != nil {
		panic(err)
	}

}
