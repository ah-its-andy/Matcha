package signer

import (
	"io"
	"net/http"
	"net/url"
	"time"
)

// A Request is an abstract representation of a http request.
type Request struct {
	Body   io.ReadSeeker
	URL    *url.URL
	Header http.Header
	Method string
}

// SigningResult is a signing result strcuture
type HMACSigningResult struct {
	Signature string
	Header    http.Header
}

// A HMACSigner is the interface for any component which will provide HMAC signature algorithm.
type HMACSigner interface {
	Sign(r *Request, exp time.Duration) *HMACSigningResult
}

// A HMACValidator is the interface for any component which will provide HMAC signature validate.
type HMACValidator interface {
	Verify(r *Request) bool
}
