// 守护进程，放在后台运行的进程
package models

import (
	"time"

	"github.com/jinzhu/gorm"
)

// 守护job结构体，DaemonJob数据表，对应的守护进程每个具体的job
type DaemonJob struct {
	gorm.Model
	Name            string      `json:"name" gorm:"index;not null"`
	GroupID         uint        `json:"groupID" grom:"index"`
	Command         StringSlice `json:"command" gorm:"type:varchar(1000)"`
	Code            string      `json:"code"  gorm:"type:TEXT"`
	ErrorMailNotify bool        `json:"errorMailNotify"`
	ErrorAPINotify  bool        `json:"errorAPINotify"`
	Status          JobStatus   `json:"status"`
	MailTo          StringSlice `json:"mailTo" gorm:"type:varchar(1000)"`
	APITo           StringSlice `json:"APITo" gorm:"type:varchar(1000)"`
	FailRestart     bool        `json:"failRestart"`
	RetryNum        int         `json:"retryNum"`
	StartAt         time.Time   `json:"startAt"`
	WorkUser        string      `json:"workUser"`
	WorkEnv         StringSlice `json:"workEnv" gorm:"type:varchar(1000)"`
	WorkDir         string      `json:"workDir"`
	CreatedUserID   uint        `json:"createdUserId"`
	CreatedUsername string      `json:"createdUsername"`
	UpdatedUserID   uint        `json:"updatedUserID"`
	UpdatedUsername string      `json:"updatedUsername"`
}
