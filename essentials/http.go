package essentials

import "net/http"

type HTTPStatus struct {
	code int
}

func (s *HTTPStatus) Int() int {
	return s.code
}

func (s *HTTPStatus) String() string {
	return http.StatusText(s.code)
}

type ActionResult interface {
	Status() *HTTPStatus
	Body() []byte
	Headers() map[string]interface{}
	Write(response http.ResponseWriter) error
}

type DefaultActionResult struct {
	code    int
	body    []byte
	headers map[string]interface{}
}

func (m *DefaultActionResult) Status() *HTTPStatus {
	return &HTTPStatus{code: m.code}
}

func (m *DefaultActionResult) Body() []byte {
	if m.body == nil {
		return make([]byte, 0)
	}
	return m.body
}

func (m *DefaultActionResult) Headers() map[string]interface{} {
	if m.headers == nil {
		return make(map[string]interface{})
	}
	return m.headers
}

func (m *DefaultActionResult) Write(response http.ResponseWriter) error {
	response.WriteHeader(m.Status().Int())
	_, err := response.Write(m.Body())
	if err != nil {
		return err
	}
	return nil
}
