package signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/FeiniuBus/log"
)

const (
	authHeaderPrefix  = "FNBUS1-HMAC-SHA256"
	timeFormat        = "20060102T150405Z"
	shortTimeFormat   = "20060102"
	emptyStringSHA256 = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

	xFeiniuBusDateHeader = "X-FeiniuBus-Date"
	authorizationHeader  = "Authorization"
)

var ignoredHeaders = rules{
	blacklist{
		mapRule{
			"Authorization": struct{}{},
			"User-Agent":    struct{}{},
		},
	},
}

// requiredSignedHeaders is a whitelist for build canonical headers.
var requiredSignedHeaders = rules{
	whitelist{
		mapRule{
			"Cache-Control":       struct{}{},
			"Content-Disposition": struct{}{},
			"Content-Encoding":    struct{}{},
			"Content-Language":    struct{}{},
			"Content-Md5":         struct{}{},
			"Content-Type":        struct{}{},
			"Expires":             struct{}{},
			"If-Match":            struct{}{},
			"If-Modified-Since":   struct{}{},
			"If-None-Match":       struct{}{},
			"If-Unmodified-Since": struct{}{},
			"Range":               struct{}{},
		},
	},
}

// allowedHoisting is a whitelist for build query headers. The boolean value
// represents whether or not it is a pattern.
var allowedQueryHoisting = inclusiveRules{
	blacklist{requiredSignedHeaders},
	patterns{"X-FeiniuBus-"},
}

type HMACSignerV1 struct {
	Key                    string
	Identifier             string
	Logger                 *log.Logger
	DisableHeaderHoisting  bool
	DisableURIPathEscaping bool

	// currentTimeFn returns the time value which represents the current time.
	currentTimeFn func() time.Time
}

// NewHMACSignerV1 returns a HMACSignerV1 pointer
func NewHMACSignerV1(id, key string, options ...func(*HMACSignerV1)) *HMACSignerV1 {
	v1 := &HMACSignerV1{
		Key:        key,
		Identifier: id,
	}

	for _, option := range options {
		option(v1)
	}

	return v1
}

type signingCtx struct {
	URL              *url.URL
	Method           string
	Body             io.ReadSeeker
	Query            url.Values
	Header           http.Header
	Time             time.Time
	ExpireTime       time.Duration
	SignedHeaderVals http.Header

	DisableURIPathEscaping bool

	key                string
	identifier         string
	formattedTime      string
	formattedShortTime string
	credentialString   string
	canonicalString    string
	bodyDigest         string
	signedHeaders      string
	canonicalHeaders   string
	stringToSign       string
	signature          string
}

func (v1 *HMACSignerV1) Sign(r *Request, exp time.Duration) *HMACSigningResult {
	currentTimeFn := v1.currentTimeFn
	if currentTimeFn == nil {
		currentTimeFn = time.Now
	}

	return v1.signWithBody(r, exp, currentTimeFn())
}

func (v1 *HMACSignerV1) signWithBody(r *Request, exp time.Duration, signTime time.Time) *HMACSigningResult {
	ctx := &signingCtx{
		URL:                    r.URL,
		Header:                 r.Header,
		Body:                   r.Body,
		Query:                  r.URL.Query(),
		Time:                   signTime,
		ExpireTime:             exp,
		Method:                 r.Method,
		DisableURIPathEscaping: v1.DisableURIPathEscaping,
		key:        v1.Key,
		identifier: v1.Identifier,
	}

	for key := range ctx.Query {
		sort.Strings(ctx.Query[key])
	}

	return ctx.build(v1.DisableHeaderHoisting)
}

func (ctx *signingCtx) build(disableHeaderHoisting bool) *HMACSigningResult {
	ctx.buildTime()
	ctx.buildCredentialString()
	ctx.buildBodyDigest()

	unsignedHeaders := ctx.Header
	if !disableHeaderHoisting {
		urlValues := url.Values{}
		urlValues, unsignedHeaders = buildQuery(allowedQueryHoisting, unsignedHeaders)
		for k := range urlValues {
			ctx.Query[k] = urlValues[k]
		}
	}

	ctx.buildCanonicalHeaders(ignoredHeaders, unsignedHeaders)
	ctx.buildCanonicalString()
	ctx.buildStringToSign()
	ctx.buildSignature()

	parts := []string{
		authHeaderPrefix + " Credential=" + ctx.identifier + "/" + ctx.credentialString,
		"SignedHeaders=" + ctx.signedHeaders,
		"Signature=" + ctx.signature,
	}

	res := &HMACSigningResult{
		Signature: ctx.signature,
		Header:    http.Header{},
	}
	res.Header.Set(xFeiniuBusDateHeader, ctx.formattedTime)
	res.Header.Set(authorizationHeader, strings.Join(parts, ","))

	return res
}

func (ctx *signingCtx) buildTime() {
	ctx.formattedTime = ctx.Time.UTC().Format(timeFormat)
	ctx.formattedShortTime = ctx.Time.UTC().Format(shortTimeFormat)
}

func buildQuery(r rule, header http.Header) (url.Values, http.Header) {
	query := url.Values{}
	unsignedHeaders := http.Header{}
	for k, h := range header {
		if r.IsValid(k) {
			query[k] = h
		} else {
			unsignedHeaders[k] = h
		}
	}

	return query, unsignedHeaders
}

func (ctx *signingCtx) buildCanonicalHeaders(r rule, header http.Header) {
	var headers []string
	headers = append(headers, "host")
	for k, v := range header {
		canonicalKey := http.CanonicalHeaderKey(k)
		if !r.IsValid(canonicalKey) {
			continue
		}
		if ctx.SignedHeaderVals == nil {
			ctx.SignedHeaderVals = make(http.Header)
		}

		lowerCaseKey := strings.ToLower(k)
		if _, ok := ctx.SignedHeaderVals[lowerCaseKey]; ok {
			ctx.SignedHeaderVals[lowerCaseKey] = append(ctx.SignedHeaderVals[lowerCaseKey], v...)
			continue
		}

		headers = append(headers, lowerCaseKey)
		ctx.SignedHeaderVals[lowerCaseKey] = v
	}
	sort.Strings(headers)

	ctx.signedHeaders = strings.Join(headers, ";")

	headerValues := make([]string, len(headers))
	for i, k := range headers {
		if k == "host" {
			headerValues[i] = "host:" + ctx.URL.Host
		} else {
			headerValues[i] = k + ":" + strings.Join(ctx.SignedHeaderVals[k], ",")
		}
	}
	stripExcessSpaces(headerValues)
	ctx.canonicalHeaders = strings.Join(headerValues, "\n")
}

func (ctx *signingCtx) buildCredentialString() {
	ctx.credentialString = strings.Join([]string{
		ctx.formattedShortTime,
		"feiniubus_request",
	}, "/")
}

func (ctx *signingCtx) buildCanonicalString() {
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

func (ctx *signingCtx) buildStringToSign() {
	ctx.stringToSign = strings.Join([]string{
		authHeaderPrefix,
		ctx.formattedTime,
		ctx.credentialString,
		hex.EncodeToString(makeSha256([]byte(ctx.canonicalString))),
	}, "\n")
}

func (ctx *signingCtx) buildSignature() {
	secret := ctx.key
	date := makeHmac([]byte("FNBUS1"+secret), []byte(ctx.formattedShortTime))
	credentials := makeHmac(date, []byte("feiniubus_request"))
	signature := makeHmac(credentials, []byte(ctx.stringToSign))
	ctx.signature = hex.EncodeToString(signature)
}

func (ctx *signingCtx) buildBodyDigest() {
	if ctx.Body == nil {
		ctx.bodyDigest = emptyStringSHA256
	} else {
		ctx.bodyDigest = hex.EncodeToString(makeSha256Reader(ctx.Body))
	}
}

func makeHmac(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func makeSha256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func makeSha256Reader(reader io.ReadSeeker) []byte {
	hash := sha256.New()
	start, _ := reader.Seek(0, io.SeekCurrent)
	defer reader.Seek(start, io.SeekStart)

	io.Copy(hash, reader)
	return hash.Sum(nil)
}

const doubleSpace = "  "

// stripExcessSpaces will rewrite the passed in slice's string values to not
// contain muliple side-by-side spaces.
func stripExcessSpaces(vals []string) {
	var j, k, l, m, spaces int
	for i, str := range vals {
		for j = len(str) - 1; j >= 0 && str[j] == ' '; j-- {
		}

		for k = 0; k < j && str[k] == ' '; k++ {
		}
		str = str[k : j+1]

		j = strings.Index(str, doubleSpace)
		if j < 0 {
			vals[i] = str
			continue
		}

		buf := []byte(str)
		for k, m, l = j, j, len(buf); k < l; k++ {
			if buf[k] == ' ' {
				if spaces == 0 {
					buf[m] = buf[k]
					m++
				}
				spaces++
			} else {
				spaces = 0
				buf[m] = buf[k]
				m++
			}
		}

		vals[i] = string(buf[:m])
	}
}
