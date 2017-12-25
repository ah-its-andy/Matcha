package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/standardcore/Matcha/essentials"
)

type Config struct {
	Address          string `json:"client_addr"`
	Port             int    `json:"port"`
	LogLevel         string `json:"log_level"`
	EnableSyslog     bool   `json:"enable_syslog"`
	SyslogFacility   string `json:"syslog_facility"`
	ConsulAddr       string `json:"consul_addr"`
	ConsulPort       int    `json:"consul_port"`
	ConsulDatacenter string `json:"consul_dc"`
	LogstashHost     string `json:"logstash_host"`
	LogstashPort     int    `json:"logstash_port"`
	ServicePrefix    string `json:"service_prefix"`

	// for sdk
	ResourcePath     string `json:"resource_path"`
	Env              string `json:"env"`
	ConnectionString string `json:"connection_string"`
	RabbitMQURI      string `json:"rabbitmq_uri"`
	Schema           string `json:"schema"`

	ConsoleOutput string `json:"console_output"`

	Parameters   map[string]string              `json:"parameters"`
	Declarations *essentials.DeclarationsConfig `json:"declarations"`
}

type ProtoAddr struct {
	Proto, Net, Addr string
}

// HTTPAddr returns the bind addresses for the HTTP server
func (c *Config) HTTPAddr() (ProtoAddr, error) {
	ip := net.ParseIP(c.Address)
	if ip == nil {
		return ProtoAddr{}, fmt.Errorf("Failed to parse IP: %s", c.Address)
	}

	a := &net.TCPAddr{IP: ip, Port: c.Port}
	return ProtoAddr{"http", a.Network(), a.String()}, nil

}

// DefaultConfig is used to return a default configuration
func DefaultConfig() *Config {
	return &Config{
		Address:          "127.0.0.1",
		Port:             5700,
		SyslogFacility:   "SYSLOG",
		LogLevel:         "INFO",
		ConsulDatacenter: "dc1",
		ConsulPort:       8500,
		Parameters:       make(map[string]string),
		Declarations:     essentials.NewDeclarationsConfig(),
	}
}

// MergeConfig merges two configurations ogether to make a single new
// configuration.
func MergeConfig(a, b *Config) *Config {
	result := *a

	if b.Address != "" {
		result.Address = b.Address
	}
	if b.Port != 0 {
		result.Port = b.Port
	}
	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}
	if b.EnableSyslog {
		result.EnableSyslog = true
	}
	if b.SyslogFacility != "" {
		result.SyslogFacility = b.SyslogFacility
	}
	if b.ConsulAddr != "" {
		result.ConsulAddr = b.ConsulAddr
	}
	if b.ConsulPort != 0 {
		result.ConsulPort = b.ConsulPort
	}
	if b.ConsulDatacenter != "" {
		result.ConsulDatacenter = b.ConsulDatacenter
	}
	if b.LogstashHost != "" {
		result.LogstashHost = b.LogstashHost
	}
	if b.LogstashPort != 0 {
		result.LogstashPort = b.LogstashPort
	}

	if b.ResourcePath != "" {
		result.ResourcePath = b.ResourcePath
	}

	if b.Env != "" {
		result.Env = b.Env
	}

	if b.ConnectionString != "" {
		result.ConnectionString = b.ConnectionString
	}

	if b.RabbitMQURI != "" {
		result.RabbitMQURI = b.RabbitMQURI
	}

	if b.Schema != "" {
		result.Schema = b.Schema
	}

	if b.Parameters != nil && len(b.Parameters) > 0 {
		if result.Parameters == nil {
			result.Parameters = make(map[string]string)
		}
		for k, v := range b.Parameters {
			result.Parameters[k] = v
		}
	}

	if result.Declarations == nil {
		result.Declarations = essentials.NewDeclarationsConfig()
	}

	if b.Declarations != nil && b.Declarations.Exchanges != nil && len(b.Declarations.Exchanges) > 0 {
		for k, v := range b.Declarations.Exchanges {
			result.Declarations.Exchanges[k] = v
		}
	}

	if b.Declarations != nil && b.Declarations.Queues != nil && len(b.Declarations.Queues) > 0 {
		for k, v := range b.Declarations.Queues {
			result.Declarations.Queues[k] = v
		}
	}

	return &result
}

// ReadConfigPaths reads the paths in the given order to load configuration
func ReadConfigPaths(paths []string) (*Config, error) {
	result := new(Config)

	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			file, _ := exec.LookPath(os.Args[0])
			abs, _ := filepath.Abs(file)
			dir := filepath.Dir(abs)
			path = filepath.Join(dir, path)
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("Error reading '%s': %s", path, err)
		}

		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("Error reading '%s': %s", path, err)
		}

		if !fi.IsDir() {
			config, err := DecodeConfig(f)
			f.Close()

			if err != nil {
				return nil, fmt.Errorf("Error decoding '%s': %s", path, err)
			}

			result = MergeConfig(result, config)
			continue
		}

		contents, err := f.Readdir(-1)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("Error reading '%s': %s", path, err)
		}

		sort.Sort(dirEnts(contents))

		for _, fi := range contents {
			if fi.IsDir() {
				continue
			}

			if !strings.HasSuffix(fi.Name(), ".json") {
				continue
			}
			if fi.Size() == 0 {
				continue
			}

			subpath := filepath.Join(path, fi.Name())
			f, err := os.Open(subpath)
			if err != nil {
				return nil, fmt.Errorf("Error reading '%s': %s", subpath, err)
			}

			config, err := DecodeConfig(f)
			f.Close()

			if err != nil {
				return nil, fmt.Errorf("Error decoding '%s': %s", subpath, err)
			}

			result = MergeConfig(result, config)
		}
	}

	return result, nil
}

// DecodeConfig reads the configuration from the given reader in JSON
// format and decodes it into a proper Config structure
func DecodeConfig(r io.Reader) (*Config, error) {
	var result Config
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

type dirEnts []os.FileInfo

func (d dirEnts) Len() int {
	return len(d)
}

func (d dirEnts) Less(i, j int) bool {
	return d[i].Name() < d[j].Name()
}

func (d dirEnts) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
