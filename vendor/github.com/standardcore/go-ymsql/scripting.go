package ymsql

type Scripting interface {
	Variables() map[string]string
	Compile() (string, error)
}
