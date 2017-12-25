package ymsql

import (
	"fmt"
	"regexp"
	"strings"
)

const varliable_regular string = `(\${)([a-zA-Z0-9_]+)(})`
const varliableName_regular string = `[a-zA-Z0-9_]+`

type YMLScripting struct {
	s         *YMLModel
	store     Store
	variables map[string]string
}

func (c *YMLScripting) Variables() map[string]string {
	return c.variables
}

func (c *YMLScripting) Compile() (string, error) {
	re, err := regexp.Compile(varliable_regular)
	if err != nil {
		return "", err
	}
	re2, err := regexp.Compile(varliableName_regular)
	if err != nil {
		return "", err
	}
	script := c.s.Script
	str := re.FindString(script)
	for str != "" {
		varName := re2.FindString(str)
		v, ok := c.Variables()[varName]
		if ok == false {
			return "", fmt.Errorf("yaml script compiler : variable %s not found", varName)
		}
		script = strings.Replace(script, str, v, -1)
		str = re.FindString(script)
	}

	return script, nil
}
