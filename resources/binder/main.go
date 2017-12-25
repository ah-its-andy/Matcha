package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const newLine string = "\r\n"

func main() {
	curr, err := filepath.Abs("./")
	if err != nil {
		panic(err)
	}
	path := filepath.Join(curr, "resources")
	codePath := filepath.Join(curr, "essentials")

	files, err := ioutil.ReadDir(filepath.Join(path, "scripts"))
	if err != nil {
		panic(err)
	}
	filename := filepath.Join(codePath, "resources.go")
	if _, err := os.Stat(filename); os.IsExist(err) {
		err = os.Remove(filename)
		if err != nil {
			panic(err)
		}
	}

	target, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(target)
	writeHeader(writer)
	writeFileList(files, writer)
	writeCtorHeader(writer)
	for _, file := range files {
		if file.IsDir() {
			return
		}
		varName := strings.ToLower(strings.Replace(file.Name(), ".", "_", -1))
		fn := filepath.Join(path, "scripts", file.Name())

		data, err := ioutil.ReadFile(fn)
		if err != nil {
			panic(err)
		}
		varcode := formatData(varName, data)
		writer.WriteString(varcode + newLine)
		writer.WriteString(newLine)
	}
	writeFooter(writer)
	err = writer.Flush()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d files embedded \r\n", len(files))
}

func formatData(name string, data []byte) string {
	dataString := base64.StdEncoding.EncodeToString(data)
	return fmt.Sprintf(`	r.Store("%s", "%s")`, name, dataString)
}

func writeHeader(writer *bufio.Writer) {
	writer.WriteString("package essentials" + newLine)
	writer.WriteString(newLine)
	writer.WriteString(`//creation_time:` + time.Now().Format(time.RFC3339))
	writer.WriteString(newLine)
}

func writeFileList(files []os.FileInfo, writer *bufio.Writer) {
	writer.WriteString(newLine)
	for _, v := range files {
		writer.WriteString(`//` + v.Name() + newLine)
	}
	writer.WriteString(newLine)
}

func writeCtorHeader(writer *bufio.Writer) {
	writer.WriteString("func NewScriptResources() *ScriptResources {" + newLine)
	writer.WriteString("	r := &ScriptResources{}" + newLine)
}

func writeFooter(writer *bufio.Writer) {
	writer.WriteString("	return r" + newLine)
	writer.WriteString("}" + newLine)
}
