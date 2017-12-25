package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/standardcore/go-logging"

	"github.com/standardcore/Matcha/essentials"

	fnet "github.com/FeiniuBus/ecosystem/net"
	"github.com/hashicorp/consul/api"
)

// consts
const (
	ServiceName = "matcha"
)

// Agent is the long running process
type Agent struct {
	sess      *essentials.Session
	config    *Config
	msrv      *MServer
	httpAddr  ProtoAddr
	wgServers sync.WaitGroup

	shutdownLock sync.Mutex
}

func New(c *Config, logger logging.Logger) (*Agent, error) {
	httpAddr, err := c.HTTPAddr()
	if err != nil {
		return nil, fmt.Errorf("Invalid HTTP bind address: %s", err)
	}

	sess, err := essentials.NewSession(c.Parameters, c.Declarations)

	if err != nil {
		return nil, err
	}

	sess.SETLogger(logger)

	a := &Agent{
		config:   c,
		httpAddr: httpAddr,
		sess:     sess,
		msrv:     NewMServer(sess),
	}

	err = a.msrv.StartUp()
	if err != nil {
		return nil, err
	}
	err = a.msrv.Configure()
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *Agent) GetSession() *essentials.Session {
	return a.sess
}

// StartSync is
func (a *Agent) StartSync() {
	config := api.DefaultConfig()
	config.Address = fmt.Sprintf("%s:%d", a.config.ConsulAddr, a.config.ConsulPort)
	config.Datacenter = a.config.ConsulDatacenter

	client, err := api.NewClient(config)
	if err != nil {
		a.sess.Logger().Errorf("agent: failed to sync start: %v\n", err)
		return
	}

	ips, err := fnet.InterfaceIPV4Addrs()
	if err != nil {
		a.sess.Logger().Errorf("agent: failed to sync start: %v\n", err)
		return
	}

	agent := client.Agent()

	reg := &api.AgentServiceRegistration{
		ID:   ServiceName + "-" + ips[0],
		Name: ServiceName,
		Tags: []string{
			"matcha",
		},
		Address:           ips[0],
		Port:              a.config.Port,
		EnableTagOverride: false,
	}

	reg.Check = &api.AgentServiceCheck{
		DeregisterCriticalServiceAfter: "10s",
		Interval:                       "5s",
		HTTP:                           fmt.Sprintf("http://%s:%d/v1/check", reg.Address, reg.Port),
		Method:                         http.MethodGet,
		Timeout:                        "1s",
	}

	err = agent.ServiceRegister(reg)
	if err != nil {
		a.sess.Logger().Errorf("agent: failed register service: [ServiceName: %s] (%s)\n", ServiceName, err)
	}
}

// Start is
func (a *Agent) Start() error {
	var e error
	//a.logger, e = log.NewLogstash(false, a.config.LogstashHost, a.config.LogstashPort)
	//a.sess.Logger().TryAddProvider(essentials.NewLogstashProvider(false, a.config.LogstashHost, a.config.LogstashPort))
	if e != nil {
		panic(e)
	}

	httpln, err := a.listenHTTP(a.httpAddr)
	if err != nil {
		return err
	}
	a.GetSession().Logger().Infoln("http listening " + a.httpAddr.Proto + "://" + a.httpAddr.Addr + ":" + a.httpAddr.Net)

	// srv := NewServerHost("http", httpln.Addr().String(), a)
	// if err := a.consume(srv); err != nil {
	// 	return err
	// }
	if err := a.serveHTTP(httpln, a.msrv); err != nil {
		return err
	}

	return nil
}

// ShutdownEndpoints terminates the HTTP servers.
func (a *Agent) ShutdownEndpoints() {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	// if a.srv == nil {
	// 	return
	// }

	if a.msrv == nil {
		return
	}

	//a.logger.Printf("agent: Stopping %s server %s", strings.ToUpper(a.srv.httpProtocol), a.srv.httpAddress)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//a.srv.HTTPServer().Shutdown(ctx)
	// if ctx.Err() == context.DeadlineExceeded {
	// 	a.logger.Warnf("agent: Timeout stopping %s server %s", strings.ToUpper(a.srv.httpProtocol), a.srv.httpAddress)
	// }

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.msrv.HTTPServer().Shutdown(ctx)
	if ctx.Err() == context.DeadlineExceeded {
		a.GetSession().Logger().Warnf("agent: Timeout stopping %s server %s", strings.ToUpper(a.msrv.httpProtocol), a.msrv.httpAddress)
	}
	//a.srv = nil
	a.msrv = nil

	a.GetSession().Logger().Println("agent: Waiting for endpoings so shut down")
	a.wgServers.Wait()
	a.GetSession().Logger().Println("agent: Endpoings down")
}

func (a *Agent) listenHTTP(addr ProtoAddr) (net.Listener, error) {
	var l net.Listener
	var err error

	switch {
	case addr.Net == "tcp" && addr.Proto == "http":
		l, err = net.Listen("tcp", addr.Addr)
		if err != nil {
			return nil, err
		}
	case addr.Net == "tcp" && addr.Proto == "https":
		return nil, errors.New("Not support https protocol")
	default:
		return nil, fmt.Errorf("%s:%s listener not supported", addr.Net, addr.Proto)
	}

	if tcpl, ok := l.(*net.TCPListener); ok {
		l = &tcpKeepAliveListener{tcpl}
	}

	return l, nil

}

// func (a *Agent) consume(srv *ServerHost) error {
// 	err := srv.RabbitMQServer().Start()
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (a *Agent) serveHTTP(l net.Listener, srv *MServer) error {
	// https://github.com/golang/go/issues/20239

	srv.httpProtocol = "http"
	if strings.Contains("*tls.listener", fmt.Sprintf("%T", l)) {
		srv.httpProtocol = "https"
	}
	notif := make(chan string)
	a.wgServers.Add(1)
	go func() {
		defer a.wgServers.Done()
		notif <- srv.httpAddress
		err := srv.HTTPServer().Serve(l)
		if err != nil && err != http.ErrServerClosed {
			a.GetSession().Logger().Print(err)
		}
	}()

	select {
	case addr := <-notif:
		if srv.httpProtocol == "https" {
			a.GetSession().Logger().Infof("agent: Started HTTPS server on %s \n", addr)
		} else {
			a.GetSession().Logger().Infof("agent: Started HTTP server on %s \n", addr)
		}
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("agent: timeout starting HTTP servers")
	}
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(30 * time.Second)
	return tc, nil
}
