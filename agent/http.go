package agent

// func ExecuteGetConfig(sess *essentials.Session) ([]byte, error) {

// }

type TransferConfig struct {
	RabbitMQ *rabbitMQConfig
}

type rabbitMQConfig struct {
	Address  string `json:"addr"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
}
