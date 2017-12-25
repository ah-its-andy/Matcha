package ymsql

type YMLModel struct {
	Name      string            `yaml:"name"`
	Variables map[string]string `yaml:"variables"`
	Script    string            `yaml:"script"`
}
