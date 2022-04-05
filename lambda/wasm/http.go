package wasm

import (
	"errors"
	"fmt"

	// TODO: would like to support http.Request, but file size explodes with net/http package
	//"net/http"

	"github.com/valyala/fastjson"
)

/*type WasmIntereceptor struct {
	core http.RoundTripper
}

func (w WasmIntereceptor) marshalRequest(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	request := &HttpRequest{
		Header: r.Header,
		Body:   body,
	}
	return string(request.Marshal()), nil
}

func (w WasmIntereceptor) unmarshalResponse(data string) (*http.Response, error) {
	r, err := UnmarshalHttpResponse(data)
	if err != nil {
		return nil, err
	}
	readCloser := io.NopCloser(bytes.NewReader(r.body))

	response := &http.Response{
		Status:     r.status,
		StatusCode: r.statusCode,
		Header:     r.header,
		Body:       readCloser,
	}
	return response, nil
}

func (w WasmIntereceptor) RoundTrip(r *http.Request) (*http.Response, error) {
	// Turn http.Request to HttpRequest
	request, err := w.marshalRequest(r)
	if err != nil {
		return nil, err
	}
	// Send it to the wasm interface
	resp := do(request)
	return w.unmarshalResponse(resp)
}*/

type HttpHeader map[string][]string

func (h HttpHeader) Add(key string, value string) {
	h[key] = append(h[key], value)
}

type HttpRequest struct {
	Method string     `json:"method"`
	Url    string     `json:"url"`
	Header HttpHeader `json:"header"`
	Body   string     `json:"body"`
}

func NewHttpRequest(method string, url string, body string) *HttpRequest {
	return &HttpRequest{Method: method, Url: url, Header: make(map[string][]string), Body: body}
}

func (h *HttpRequest) Marshal() []byte {
	return []byte(fmt.Sprintf(`{
		"method": "%s",
		"url": "%s",
		"header": %s,
		"body": "%s"
	}`,
		h.Method,
		h.Url,
		MarshalStringListMap(h.Header),
		string(h.Body)))
}

type HttpResponse struct {
	Header     HttpHeader
	Status     string
	StatusCode int
	Body       []byte
}

func UnmarshalHttpResponse(data string) (*HttpResponse, error) {
	var p fastjson.Parser
	v, err := p.Parse(string(data))
	if err != nil {
		return nil, err
	}
	header := HttpHeader{}
	for _, av := range v.GetArray("header") {
		ho, err := av.Object()
		if err != nil {
			return nil, err
		}
		ho.Visit(func(key []byte, v *fastjson.Value) {
			header.Add(string(key), v.Get(string(key)).String())
		})
	}
	res := &HttpResponse{
		Header:     header,
		Status:     v.Get("status").String(),
		StatusCode: v.Get("statusCode").GetInt(),
		Body:       v.Get("body").GetStringBytes(),
	}
	return res, nil
}

func Do(request *HttpRequest) (string, error) {
	/*body, err := io.ReadAll(request.Body)
	if err != nil {
		return err
	}
	httpRequest := &HttpRequest{
		header: request.Header,
		body:   body,
	}*/
	m := request.Marshal()
	if do(string(m)) {
		return result, nil
	}
	return "", errors.New(result)
}

//export do
func do(request string) bool
