package signer

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
)

type RSACertFileAccessor struct {
	absolutePath string
}

func ResolveFileURI(uri string) (RSACertAccessor, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "files" {
		return nil, fmt.Errorf("'files://' is expected, bus the '%s://' is provided", u.Scheme)
	}
	if u.Host == "~" {
		currentUser, err := user.Current()
		if err != nil {
			return nil, err
		}
		return &RSACertFileAccessor{absolutePath: filepath.Join(currentUser.HomeDir, u.Path)}, nil
	}
	absolutePath, err := filepath.Abs(filepath.Join(u.Host, u.Path))
	if err != nil {
		return nil, err
	}
	return &RSACertFileAccessor{absolutePath: absolutePath}, nil
}

func (u *RSACertFileAccessor) Upload(body []byte) error {
	f, err := os.Create(u.absolutePath)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)
	_, err = writer.Write(body)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (u *RSACertFileAccessor) Download() ([]byte, error) {
	f, err := os.Open(u.absolutePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
