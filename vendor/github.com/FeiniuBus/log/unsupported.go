// +build windows plan9

package log

import (
	"fmt"
)

func NewSyslogSyncer() (*SyslogSyncer, error) {
	return nil, fmt.Errorf("Platform does not support syslog")
}

func NewSyslog(debugLevel bool, app string) (*Logger, error) {
	return nil, fmt.Errorf("Platform does not support syslog")
}
