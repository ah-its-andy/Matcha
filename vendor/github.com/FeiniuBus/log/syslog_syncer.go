package log

import (
	"errors"
	"net"
	"time"
)

const localDeadline = 20 * time.Millisecond

type SyslogSyncer struct {
	conn net.Conn
}

func (s *SyslogSyncer) Write(p []byte) (n int, err error) {
	if s.conn != nil {
		if n, err = s.conn.Write(p); err == nil {
			return n, err
		}
	}

	if err = s.connect(); err != nil {
		return 0, err
	}

	return s.conn.Write(p)
}

func (s *SyslogSyncer) Sync() error {
	return nil
}

func (s *SyslogSyncer) connect() error {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	logTypes := []string{"unixgram", "unix"}
	logPaths := []string{"/dev/log", "/var/run/syslog", "/var/run/log"}

	for _, network := range logTypes {
		for _, path := range logPaths {
			conn, err := net.DialTimeout(network, path, localDeadline)
			if err != nil {
				continue
			} else {
				s.conn = conn
				return nil
			}
		}
	}

	return errors.New("Unix syslog delivery error")
}
