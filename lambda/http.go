package lambda

import "net/http"

type HttpRequest struct {
	Method string              `json:"method"`
	Url    string              `json:"url"`
	Header map[string][]string `json:"header"`
	Body   string              `json:"body"`
}

type WasmResponseWriter struct {
	header     http.Header
	statusCode int
	body       []byte
}

func NewWasmResponseWriter() *WasmResponseWriter {
	return &WasmResponseWriter{
		header:     make(http.Header),
		statusCode: 200,
	}
}
func (rw *WasmResponseWriter) Header() http.Header {
	return rw.header
}

func (rw *WasmResponseWriter) Write(b []byte) (int, error) {
	l := len(b)
	rw.body = make([]byte, l)
	copy(rw.body[:l], b)
	return l, nil
}

func (rw *WasmResponseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
}
