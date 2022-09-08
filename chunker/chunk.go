/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chunker

import (
	"bufio"
	"bytes"
	"compress/gzip"
	encjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"unicode"

	"github.com/outcaste-io/outserv/ee/enc"
	gqlSchema "github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/x"

	"github.com/pkg/errors"
)

// Chunker is JSON chunker
type Chunker struct {
	nqs    *NQuadBuffer
	inList bool
}

func (jc *Chunker) NQuads() *NQuadBuffer {
	return jc.nqs
}

// NewChunker returns a new chunker for the specified format.
func NewChunker(schema *gqlSchema.Schema, batchSize int) *Chunker {
	return &Chunker{
		nqs: NewNQuadBuffer(schema, batchSize),
	}
}

// Chunk tries to consume multiple top-level maps from the reader until a size threshold is
// reached, or the end of file is reached.
func (jc *Chunker) Chunk(r *bufio.Reader) (*bytes.Buffer, error) {
	ch, err := jc.nextRune(r)
	if err != nil {
		return nil, err
	}
	// If the file starts with a list rune [, we set the inList flag, and keep consuming maps
	// until we reach the threshold.
	switch {
	case ch == '[':
		jc.inList = true
	case ch == '{':
		// put the rune back for it to be consumed in the consumeMap function
		if err := r.UnreadRune(); err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("file is not JSON")
	}

	out := new(bytes.Buffer)
	if _, err := out.WriteRune('['); err != nil {
		return nil, err
	}
	hasMapsBefore := false
	for out.Len() < 1e5 {
		if hasMapsBefore {
			if _, err := out.WriteRune(','); err != nil {
				return nil, err
			}
		}
		if err := jc.consumeMap(r, out); err != nil {
			return nil, err
		}
		hasMapsBefore = true

		// handle the legal termination cases, by checking the next rune after the map
		ch, err := jc.nextRune(r)
		if err == io.EOF {
			// handles the EOF case, return the buffer which represents the top level map
			if jc.inList {
				return nil, errors.Errorf("JSON file ends abruptly, expecting ]")
			}

			if _, err := out.WriteRune(']'); err != nil {
				return nil, err
			}
			return out, io.EOF
		} else if err != nil {
			return nil, err
		}

		if ch == ']' {
			if !jc.inList {
				return nil, errors.Errorf("JSON map is followed by an extraneous ]")
			}

			// validate that there are no more non-space chars after the ]
			if slurpSpace(r) != io.EOF {
				// This is a situation where we had [{...},{...}] [{...},{...}]
				// within the same file. In that case, we can absorb the leading
				// `[` and continue with the map.
				ch, err = jc.nextRune(r)
				if err != nil {
					return nil, fmt.Errorf("Got err after ]: %v", err)
				}
				if ch == '[' {
					continue // Now expecting a {..}
				}
				return nil, fmt.Errorf("Unexpected rune after ]: %q", ch)
			}

			if _, err := out.WriteRune(']'); err != nil {
				return nil, err
			}
			return out, io.EOF
		}

		// In the non termination cases, ensure at least one map has been consumed, and
		// the only allowed char after the map is ",".
		if out.Len() == 1 { // 1 represents the [ inserted before the for loop
			return nil, errors.Errorf("Illegal rune found \"%c\", expecting {", ch)
		}
		if ch != ',' {
			return nil, errors.Errorf("JSON map is followed by illegal rune \"%c\"", ch)
		}
	}
	if _, err := out.WriteRune(']'); err != nil {
		return nil, err
	}
	return out, nil
}

// consumeMap consumes the next map from the reader, and stores the result into the buffer out.
// After ignoring spaces, if the reader does not begin with {, no rune will be consumed
// from the reader.
func (jc *Chunker) consumeMap(r *bufio.Reader, out *bytes.Buffer) error {
	// Just find the matching closing brace. Let the JSON-to-nquad parser in the mapper worry
	// about whether everything in between is valid JSON or not.
	depth := 0
	for {
		ch, err := jc.nextRune(r)
		if err != nil {
			return errors.New("Malformed JSON")
		}
		if depth == 0 && ch != '{' {
			// We encountered a beginning rune that's not {,
			// unread the char and return without consuming anything.
			if err := r.UnreadRune(); err != nil {
				return err
			}
			return nil
		}

		if _, err := out.WriteRune(ch); err != nil {
			return err
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
		case '"':
			if err := slurpQuoted(r, out); err != nil {
				return err
			}
		default:
			// We just write the rune to out, and let the Go JSON parser do its job.
		}
		if depth <= 0 {
			break
		}
	}
	return nil
}

// nextRune ignores any number of spaces that may precede a rune
func (*Chunker) nextRune(r *bufio.Reader) (rune, error) {
	if err := slurpSpace(r); err != nil {
		return ' ', err
	}
	ch, _, err := r.ReadRune()
	if err != nil {
		return ' ', err
	}
	return ch, nil
}

func (jc *Chunker) Parse(chunkBuf *bytes.Buffer, typ *gqlSchema.Type) error {
	if chunkBuf == nil || chunkBuf.Len() == 0 {
		return nil
	}
	return jc.nqs.FastParseJSON(chunkBuf.Bytes(), typ, SetNquads)
}

func slurpSpace(r *bufio.Reader) error {
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			return err
		}
		if !unicode.IsSpace(ch) {
			x.Check(r.UnreadRune())
			return nil
		}
	}
}

func slurpQuoted(r *bufio.Reader, out *bytes.Buffer) error {
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			return err
		}
		if _, err := out.WriteRune(ch); err != nil {
			return err
		}

		if ch == '\\' {
			// Pick one more rune.
			esc, _, err := r.ReadRune()
			if err != nil {
				return err
			}
			if _, err := out.WriteRune(esc); err != nil {
				return err
			}
			continue
		}
		if ch == '"' {
			return nil
		}
	}
}

// FileReader returns an open reader on the given file. Gzip-compressed input is detected
// and decompressed automatically even without the gz extension. The key, if non-nil,
// is used to decrypt the file. The caller is responsible for calling the returned cleanup
// function when done with the reader.
func FileReader(file string, key x.Sensitive) (*bufio.Reader, func()) {
	var f *os.File
	var err error
	if file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(file)
	}

	x.Check(err)

	return StreamReader(file, key, f)
}

// StreamReader returns a bufio given a ReadCloser. The file is passed just to check for .gz files
func StreamReader(file string, key x.Sensitive, f io.ReadCloser) (
	rd *bufio.Reader, cleanup func()) {
	cleanup = func() { _ = f.Close() }

	if filepath.Ext(file) == ".gz" {
		r, err := enc.GetReader(key, f)
		x.Check(err)
		gzr, err := gzip.NewReader(r)
		x.Check(err)
		rd = bufio.NewReader(gzr)
		cleanup = func() { _ = f.Close(); _ = gzr.Close() }
	} else {
		rd = bufio.NewReader(f)
		buf, _ := rd.Peek(512)

		typ := http.DetectContentType(buf)
		if typ == "application/x-gzip" {
			gzr, err := gzip.NewReader(rd)
			x.Check(err)
			rd = bufio.NewReader(gzr)
			cleanup = func() { _ = f.Close(); _ = gzr.Close() }
		}
	}

	return rd, cleanup
}

// IsJSONData returns true if the reader, which should be at the start of the stream, is reading
// a JSON stream, false otherwise.
func IsJSONData(r *bufio.Reader) (bool, error) {
	buf, err := r.Peek(512)
	if err != nil && err != io.EOF {
		return false, err
	}

	de := encjson.NewDecoder(bytes.NewReader(buf))
	_, err = de.Token()

	return err == nil, nil
}
