package util

import (
	"net"
	"strings"
)

// InternalIP return internal ip.
func InternalIP() string {
	inters, err := net.Interfaces() // 获取主机所有的接口
	if err != nil {
		return ""
	}
	for _, inter := range inters {
		if inter.Flags&net.FlagUp != 0 && !strings.HasPrefix(inter.Name, "lo") {
			addrs, err := inter.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil { // 获取的IP可以转成IPv4
						return ipnet.IP.String()
					}
				}
			}
		}
	}
	return ""
}
