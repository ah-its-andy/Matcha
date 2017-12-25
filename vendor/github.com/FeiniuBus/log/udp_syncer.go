package log

import (
	"fmt"
	"net"
	"time"
)

type UDPSyncer struct {
	host    string
	port    int
	timeout int
	conn    net.Conn
}

func NewUDPSyncer(host string, port int, timeout int) (*UDPSyncer, error) {
	s := &UDPSyncer{
		host:    host,
		port:    port,
		timeout: timeout,
	}

	err := s.connect()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *UDPSyncer) connect() error {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	url := fmt.Sprintf("%s:%d", s.host, s.port)
	addr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		return err
	}

	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	if c != nil {
		deadline := time.Now().Add(time.Duration(s.timeout) * time.Second)
		c.SetDeadline(deadline)
		c.SetReadDeadline(deadline)
		c.SetWriteDeadline(deadline)
	}

	s.conn = c
	return nil
}

func (s *UDPSyncer) Write(p []byte) (n int, err error) {
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

func (s *UDPSyncer) Sync() error {
	return nil
}
