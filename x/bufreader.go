package x

import (
	"io"

	"github.com/outcaste-io/ristretto/z"
)

type BufReader struct {
	r      io.ReadCloser
	curBuf []byte
	ch     chan []byte
	bufs   [2][]byte
}

func NewBufReader(r io.ReadCloser, bufSize int) *BufReader {
	br := &BufReader{
		r:  r,
		ch: make(chan []byte), // unbuffered
	}
	for i := 0; i < len(br.bufs); i++ {
		br.bufs[i] = z.Calloc(bufSize, "BufReader")
	}
	go br.fill()
	return br
}

func (br *BufReader) Close() {
	br.r.Close()
	for i := 0; i < len(br.bufs); i++ {
		z.Free(br.bufs[i])
	}
}

func (br *BufReader) fill() {
	for i := 0; ; i++ {
		buf := br.bufs[i%2]

		n, err := io.ReadFull(br.r, buf)
		if err == io.EOF {
			close(br.ch)
			return
		}
		if err == io.ErrUnexpectedEOF {
			buf = buf[:n]
			br.ch <- buf
			close(br.ch)
			return
		}
		Check(err)
		br.ch <- buf
	}
}

func (br *BufReader) ReadByte() (byte, error) {
	var buf [1]byte
	n, err := br.Read(buf[:])
	if err != nil {
		return 0x0, err
	}
	AssertTrue(n == 1)
	return buf[0], nil
}

func (br *BufReader) Read(p []byte) (int, error) {
	if len(br.curBuf) == 0 {
		// check if ch is closed.
		br.curBuf = <-br.ch
		if br.curBuf == nil {
			// channel has been closed.
			return 0, io.EOF
		}
	}
	n := copy(p, br.curBuf)
	br.curBuf = br.curBuf[n:]
	return n, nil
}
