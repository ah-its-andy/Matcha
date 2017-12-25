package signer

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/FeiniuBus/log"
)

type HMACValidatorV1 struct {
	Logger                 *log.Logger
	DisableHeaderHoisting  bool
	DisableURIPathEscaping bool

	h func(string) (string, error)
}

// NewHMACValidatorV1 returns a HMACValidatorV1 pointer
func NewHMACValidatorV1(h func(string) (string, error), options ...func(*HMACValidatorV1)) *HMACValidatorV1 {
	v1 := &HMACValidatorV1{
		h: h,
	}

	for _, option := range options {
		option(v1)
	}

	return v1
}

func (v1 *HMACValidatorV1) Verify(r *Request) bool {
	authString := r.Header.Get(authorizationHeader)
	if len(authString) == 0 {
		return false
	}

	parts := strings.FieldsFunc(authString, func(r rune) bool { return r == ' ' })
	if len(parts) != 2 {
		return false
	}
	if parts[0] != authHeaderPrefix {
		return false
	}

	return v1.verifyWithBody(r, parts[1])
}

func (v1 *HMACValidatorV1) verifyWithBody(r *Request, auth string) bool {
	ctx := &verifyCtx{
		URL:                    r.URL,
		Header:                 r.Header,
		Body:                   r.Body,
		Query:                  r.URL.Query(),
		Method:                 r.Method,
		DisableURIPathEscaping: v1.DisableURIPathEscaping,
		AuthString:             auth,
		Logger:                 v1.Logger,
		GetKeyFn:               v1.h,
	}

	for key := range ctx.Query {
		sort.Strings(ctx.Query[key])
	}

	return ctx.verify(v1.DisableHeaderHoisting)
}

type verifyCtx struct {
	URL                    *url.URL
	Method                 string
	Body                   io.ReadSeeker
	Query                  url.Values
	Header                 http.Header
	AuthString             string
	DisableURIPathEscaping bool
	Logger                 *log.Logger
	GetKeyFn               func(string) (string, error)

	credential         string
	credentialString   string
	canonicalHeaders   string
	canonicalString    string
	stringToSign       string
	bodyDigest         string
	identifier         string
	key                string
	formattedTime      string
	formattedShortTime string
	credSuffix         string
	signedHeaders      string
	signedHeadersArr   []string
	signature          []byte
	clientSignature    []byte

	err error
}

func (ctx *verifyCtx) verify(disableHeaderHoisting bool) bool {
	ctx.formattedTime = ctx.Header.Get(xFeiniuBusDateHeader)
	if ctx.formattedTime == "" {
		ctx.log("Can't resolved formattedTime header")
		return false
	}

	ctx.parseAuthString()
	if ctx.err != nil {
		ctx.log("Verfiy signature failed: %v", ctx.err)
		return false
	}

	ctx.parseCredential()
	if ctx.err != nil {
		ctx.log("Parse Credential string failed: %v", ctx.err)
		return false
	}

	ctx.buildCredentialString()
	ctx.buildBodyDigest()

	if !disableHeaderHoisting {
		urlValues, _ := buildQuery(allowedQueryHoisting, ctx.Header)
		for k := range urlValues {
			ctx.Query[k] = urlValues[k]
		}
	}

	ctx.buildCanonicalHeaders()
	ctx.buildCanonicalString()
	ctx.buildStringToSign()
	ctx.buildSignature()

	return bytes.Equal(ctx.signature, ctx.clientSignature)
}

func (ctx *verifyCtx) buildSignature() {
	secret := ctx.key
	date := makeHmac([]byte("FNBUS1"+secret), []byte(ctx.formattedShortTime))
	credentials := makeHmac(date, []byte(ctx.credSuffix))
	signature := makeHmac(credentials, []byte(ctx.stringToSign))
	ctx.signature = signature
}

func (ctx *verifyCtx) buildStringToSign() {
	ctx.stringToSign = strings.Join([]string{
		authHeaderPrefix,
		ctx.formattedTime,
		ctx.credentialString,
		hex.EncodeToString(makeSha256([]byte(ctx.canonicalString))),
	}, "\n")
}

func (ctx *verifyCtx) buildCanonicalString() {
	uri := ctx.URL.Path
	if !ctx.DisableURIPathEscaping {
		uri = ctx.URL.EscapedPath()
	}

	ctx.canonicalString = strings.Join([]string{
		ctx.Method,
		uri,
		ctx.URL.RawQuery,
		ctx.canonicalHeaders + "\n",
		ctx.signedHeaders,
		ctx.bodyDigest,
	}, "\n")
}

func (ctx *verifyCtx) buildCanonicalHeaders() {
	headerValues := make([]string, len(ctx.signedHeadersArr))
	for i, k := range ctx.signedHeadersArr {
		if k == "host" {
			headerValues[i] = "host:" + ctx.URL.Host
		} else {
			for hk, hv := range ctx.Header {
				lowerCaseKey := strings.ToLower(hk)
				if lowerCaseKey != k {
					continue
				}
				headerValues[i] = k + ":" + strings.Join(hv, ",")
			}
		}
	}

	stripExcessSpaces(headerValues)
	ctx.canonicalHeaders = strings.Join(headerValues, "\n")
}

func (ctx *verifyCtx) parseAuthString() {
	parts := strings.FieldsFunc(ctx.AuthString, func(r rune) bool { return r == ',' })
	if len(parts) != 3 {
		ctx.err = fmt.Errorf("Error auth string: %s", ctx.AuthString)
		return
	}

	value := strings.FieldsFunc(parts[0], func(r rune) bool { return r == '=' })
	if len(value) != 2 {
		ctx.err = fmt.Errorf("Error Credential string: %s", parts[0])
		return
	}
	ctx.credential = value[1]

	value = strings.FieldsFunc(parts[1], func(r rune) bool { return r == '=' })
	if len(value) != 2 {
		ctx.err = fmt.Errorf("Error SignedHeaders string: %s", parts[1])
		return
	}

	ctx.signedHeaders = value[1]
	ctx.signedHeadersArr = strings.Split(value[1], ";")

	value = strings.FieldsFunc(parts[2], func(r rune) bool { return r == '=' })
	if len(value) != 2 {
		ctx.err = fmt.Errorf("Error Signature string: %s: ", parts[2])
		return
	}

	ctx.clientSignature, ctx.err = hex.DecodeString(value[1])
}

func (ctx *verifyCtx) parseCredential() {
	values := strings.FieldsFunc(ctx.credential, func(r rune) bool { return r == '/' })
	if len(values) != 3 {
		ctx.err = fmt.Errorf("Error Credential value: %s", ctx.credential)
		return
	}

	ctx.identifier = values[0]
	ctx.formattedShortTime = values[1]
	ctx.credSuffix = values[2]
	ctx.key, ctx.err = ctx.GetKeyFn(ctx.identifier)
}

func (ctx *verifyCtx) buildCredentialString() {
	ctx.credentialString = strings.Join([]string{
		ctx.formattedShortTime,
		ctx.credSuffix,
	}, "/")
}

func (ctx *verifyCtx) buildBodyDigest() {
	if ctx.Body == nil {
		ctx.bodyDigest = emptyStringSHA256
	} else {
		ctx.bodyDigest = hex.EncodeToString(makeSha256Reader(ctx.Body))
	}
}

func (ctx *verifyCtx) log(format string, args ...interface{}) {
	if ctx.Logger != nil {
		ctx.Logger.Warnf(format, args)
	}
}
