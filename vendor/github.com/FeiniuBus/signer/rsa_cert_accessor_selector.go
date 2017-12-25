package signer

import (
	"fmt"
	"net/url"
)

func ParseURI(urlStr string) (RSACertAccessor, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "s3" {
		return ParseS3URI(urlStr)
	} else if u.Scheme == "files" {
		return ResolveFileURI(urlStr)
	} else {
		return nil, fmt.Errorf("unknown schema of %s", u.Scheme)
	}
}
