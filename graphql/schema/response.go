// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/x"
)

// GraphQL spec on response is here:
// https://graphql.github.io/graphql-spec/June2018/#sec-Response

// GraphQL spec on errors is here:
// https://graphql.github.io/graphql-spec/June2018/#sec-Errors

// GraphQL spec on extensions says just this:
// The response map may also contain an entry with key extensions. This entry, if set, must have a
// map as its value. This entry is reserved for implementors to extend the protocol however they
// see fit, and hence there are no additional restrictions on its contents.

// Response represents a GraphQL response
type Response struct {
	Errors     x.GqlErrorList
	Data       bytes.Buffer
	Extensions *Extensions
	Header     http.Header
	dataIsNull bool
}

// ErrorResponse formats an error as a list of GraphQL errors and builds
// a response with that error list and no data.  Because it doesn't add data, it
// should be used before starting execution - GraphQL spec requires no data if an
// error is detected before execution begins.
func ErrorResponse(err error) *Response {
	return &Response{
		Errors: AsGQLErrors(err),
	}
}

// GetExtensions returns a *Extensions
func (r *Response) GetExtensions() *Extensions {
	if r == nil {
		return nil
	}
	return r.Extensions
}

// WithError generates GraphQL errors from err and records those in r.
func (r *Response) WithError(err error) {
	if err == nil {
		return
	}

	if !x.Config.GraphQL.Debug && strings.Contains(err.Error(), "authorization failed") {
		return
	}

	if !x.Config.GraphQL.Debug && strings.Contains(err.Error(), "GraphQL debug:") {
		return
	}

	r.Errors = append(r.Errors, AsGQLErrors(err)...)
}

// AddData adds p to r's data buffer.
//
// If p is empty or r.SetDataNull() has been called earlier, the call has no effect.
//
// If r.Data is empty before the call, then r.Data becomes {p}.
// If r.Data contains data it always looks like {f,g,...}, and
// adding to that results in {f,g,...,p}.
func (r *Response) AddData(p []byte) {
	if r == nil || r.dataIsNull || len(p) == 0 {
		return
	}

	if r.Data.Len() == 0 {
		x.Check2(r.Data.Write(p))
		return
	}

	// The end of the buffer is always the closing `}`
	r.Data.Truncate(r.Data.Len() - 1)
	x.Check2(r.Data.WriteRune(','))

	x.Check2(r.Data.Write(p[1 : len(p)-1]))
	x.Check2(r.Data.WriteRune('}'))
}

// SetDataNull sets r's data buffer to contain the bytes representing a null.
// Once this has been called on r, any further call to AddData has no effect.
func (r *Response) SetDataNull() {
	r.dataIsNull = true
	r.Data.Reset()
	x.Check2(r.Data.Write(JsonNull))
}

// MergeExtensions merges the extensions given in ext to r.
// If r.Extensions is nil before the call, then r.Extensions becomes ext.
// Otherwise, r.Extensions gets merged with ext.
func (r *Response) MergeExtensions(ext *Extensions) {
	if r == nil {
		return
	}

	if r.Extensions == nil {
		r.Extensions = ext
		return
	}

	r.Extensions.Merge(ext)
}

// WriteTo writes the GraphQL response as unindented JSON to w
// and returns the number of bytes written and error, if any.
func (r *Response) WriteTo(w io.Writer) (int64, error) {
	js, err := json.Marshal(r.Output())

	if err != nil {
		msg := "Internal error - failed to marshal a valid JSON response"
		glog.Errorf("%+v", errors.Wrap(err, msg))
		js = []byte(fmt.Sprintf(
			`{ "errors": [{"message": "%s"}], "data": null }`, msg))
	}

	i, err := w.Write(js)
	return int64(i), err
}

// Output returns json interface of the response
func (r *Response) Output() interface{} {
	if r == nil {
		return struct {
			Errors json.RawMessage `json:"errors,omitempty"`
			Data   json.RawMessage `json:"data,omitempty"`
		}{
			Errors: []byte(`[{"message": "Internal error - no response to write."}]`),
			Data:   JsonNull,
		}
	}

	res := struct {
		Errors     []*x.GqlError   `json:"errors,omitempty"`
		Data       json.RawMessage `json:"data,omitempty"`
		Extensions *Extensions     `json:"extensions,omitempty"`
	}{
		Errors: r.Errors,
		Data:   r.Data.Bytes(),
	}

	if x.Config.GraphQL.Extensions {
		res.Extensions = r.Extensions
	}
	return res
}

// Extensions represents GraphQL extensions
type Extensions struct {
	TouchedUids uint64 `json:"touched_uids,omitempty"`
}

// GetTouchedUids returns TouchedUids
func (e *Extensions) GetTouchedUids() uint64 {
	if e == nil {
		return 0
	}
	return e.TouchedUids
}

// Merge merges ext with e
func (e *Extensions) Merge(ext *Extensions) {
	if e == nil || ext == nil {
		return
	}

	e.TouchedUids += ext.TouchedUids
}
