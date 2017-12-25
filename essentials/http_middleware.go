package essentials

import (
	"fmt"
	"net/http"
)

type RoutedMiddleware interface {
	Middleware
	CreateScope(response http.ResponseWriter, request *http.Request) RoutedMiddleware
	Handler() *HTTPMiddlewareHandler

	Session() *Session

	Ok(body []byte) ActionResult
	NoContent() ActionResult
	BadRequest(message string) ActionResult
	Error(err error) ActionResult
	ErrorWrap(message string, err error) ActionResult
}

type HTTPMiddleware struct {
	RoutedMiddleware
	http.ResponseWriter
	*http.Request

	sess *Session
}

func NewHTTPMiddleware(sess *Session, impl RoutedMiddleware) *HTTPMiddleware {
	return &HTTPMiddleware{
		sess:             sess,
		RoutedMiddleware: impl,
	}
}

func (m *HTTPMiddleware) Session() *Session {
	return m.sess
}

func (m *HTTPMiddleware) Ok(body []byte) ActionResult {
	return &DefaultActionResult{
		code: 200,
		body: body,
	}
}

func (m *HTTPMiddleware) NoContent() ActionResult {
	return &DefaultActionResult{
		code: 204,
	}
}

func (m *HTTPMiddleware) BadRequest(message string) ActionResult {
	return &DefaultActionResult{
		code: 400,
		body: []byte(message),
	}
}

func (m *HTTPMiddleware) Error(err error) ActionResult {
	return &DefaultActionResult{
		code: 500,
		body: []byte(err.Error()),
	}
}

func (m *HTTPMiddleware) ErrorWrap(message string, err error) ActionResult {
	return &DefaultActionResult{
		code: 500,
		body: []byte(fmt.Sprintf("%s : %s", message, err.Error())),
	}
}

func (m *HTTPMiddleware) Handler() *HTTPMiddlewareHandler {
	return &HTTPMiddlewareHandler{sess: m.sess, m: m}
}

type HTTPMiddlewareHandler struct {
	sess *Session
	m    RoutedMiddleware
}

func (handler *HTTPMiddlewareHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	middleware := handler.m.CreateScope(writer, request)
	_ = middleware.Execute(NewMatchaContext(handler.sess))
}

type CommonHTTPMiddleware struct {
	*HTTPMiddleware

	handle *func(*MatchaContext, http.ResponseWriter, *http.Request) error
}

func NewCommonHTTPMiddleware(sess *Session, handle *func(*MatchaContext, http.ResponseWriter, *http.Request) error) RoutedMiddleware {
	m := &CommonHTTPMiddleware{handle: handle}
	m.HTTPMiddleware = NewHTTPMiddleware(sess, m)
	return m
}

func (m *CommonHTTPMiddleware) CreateScope(response http.ResponseWriter, request *http.Request) RoutedMiddleware {
	s := NewCommonHTTPMiddleware(m.Session(), m.handle).(*CommonHTTPMiddleware)
	s.ResponseWriter = response
	s.Request = request
	return s
}

func (m *CommonHTTPMiddleware) Execute(ctx *MatchaContext) error {
	f := *m.handle
	err := f(ctx, m.ResponseWriter, m.Request)
	if err != nil {
		return err
	}
	return nil
}
