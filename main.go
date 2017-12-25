package main

import (
	"os"

	_ "github.com/lib/pq"
)

func main() {

	args := os.Args[1:]
	cmd := new(Command)
	code := cmd.Run(args)

	os.Exit(code)
}
