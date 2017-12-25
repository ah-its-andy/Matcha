package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/standardcore/Matcha/essentials"
	logging "github.com/standardcore/go-logging"

	"github.com/standardcore/Matcha/agent"
)

// AppendSliceValue implements the flag.Value interface and allows multiple
// calls to the same variable to append a list.
type AppendSliceValue []string

func (s *AppendSliceValue) String() string {
	return strings.Join(*s, ",")
}

// Set is
func (s *AppendSliceValue) Set(value string) error {
	if *s == nil {
		*s = make([]string, 0, 1)
	}

	*s = append(*s, value)
	return nil
}

// Command runs a FeiniuPay agent.
type Command struct {
	args []string
	//logger *log.Logger
	logger logging.Logger
}

// Run returns
func (cmd *Command) Run(args []string) int {
	code := cmd.run(args)
	if cmd.logger != nil {
		//defer cmd.logger.Sync()
		cmd.logger.Println("Exit code: ", code)
	}
	return code
}

func (cmd *Command) run(args []string) int {
	cmd.args = args
	config := cmd.readConfig()

	if config == nil {
		return 1
	}

	if cmd.logger == nil {
		cmd.logger = logging.NewLogger()
	}
	cmd.logger.TryAddProvider(essentials.NewLogstashProvider(false, "office.feelbus.cn", 7789))

	//cmd.logger, _ = log.NewLogstash(false, "office.feelbus.cn", 7789)
	//defer cmd.logger.Sync()

	cmd.logger.Println("Starting matcha agent...")
	agent, err := agent.New(config, cmd.logger)
	if err != nil {
		cmd.logger.Errorf("Error creating agent: %s\n", err)
		return 1
	}

	if err := agent.Start(); err != nil {
		cmd.logger.Errorf("Error starting agent: %s\n", err)
		return 1
	}

	agent.StartSync()
	defer agent.ShutdownEndpoints()

	cmd.logger.Println("matcha agent running!")
	//cmd.logger.Sync()

	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)

	for {
		var sig os.Signal

		select {
		case s := <-signalCh:
			sig = s
		}

		switch sig {
		case syscall.SIGPIPE:
			continue

		default:
			return 0
		}
	}
}

func (cmd *Command) readConfig() *agent.Config {
	var cmdCfg agent.Config
	var cfgFiles []string
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	f.Var((*AppendSliceValue)(&cfgFiles), "config-file", "Path to a JSON file to read configuration from. This can be specified multiple times.")
	f.Var((*AppendSliceValue)(&cfgFiles), "config-dir", "Path to a directory to read configuration files from.")

	f.StringVar(&cmdCfg.ConsoleOutput, "console_output", "false", "Show log in console.")
	f.StringVar(&cmdCfg.Address, "client", "", "Sets the address to bind for client access. This includes HTTP and HTTPS (if configured).")
	f.IntVar(&cmdCfg.Port, "http-port", 0, "Sets the HTTP API port to listen on.")
	f.StringVar(&cmdCfg.LogLevel, "log-level", "", "Log level of the system.")
	f.BoolVar(&cmdCfg.EnableSyslog, "syslog", false, "Enables logging to syslog.")
	f.StringVar(&cmdCfg.ConsulAddr, "consul_addr", "", "consul client address.")
	f.IntVar(&cmdCfg.ConsulPort, "consul_port", 0, "consul client port")
	f.StringVar(&cmdCfg.ConsulDatacenter, "consul_dc", "", "consul datacenter")
	f.StringVar(&cmdCfg.LogstashHost, "logstash_host", "", "Logstash host address")
	f.IntVar(&cmdCfg.LogstashPort, "logstash_port", 0, "Logstash port")
	//f.StringVar(&cmdCfg.ServicePrefix, "service_prefix", "", "Service Prefix")

	if err := f.Parse(cmd.args); err != nil {
		fmt.Println(err.Error())
		return nil
	}

	if cmd.logger == nil {
		cmd.logger = logging.NewLogger()
	}

	if cmdCfg.ConsoleOutput == "true" {
		cmd.logger.TryAddProvider(logging.NewConsoleProvider())
	}

	cfg := agent.DefaultConfig()

	if len(cfgFiles) > 0 {
		fileConfig, err := agent.ReadConfigPaths(cfgFiles)
		if err != nil {
			fmt.Println(err.Error())
			return nil
		}

		cfg = agent.MergeConfig(cfg, fileConfig)
	}

	cfg = agent.MergeConfig(cfg, &cmdCfg)

	return cfg
}
