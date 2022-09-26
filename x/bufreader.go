package x

import (
	"encoding/binary"
	"io"
)

type BufReader struct {
	r      io.Reader
	u64buf []byte
	curBuf []byte
	ch     chan []byte
	bufs   [2][]byte
}

func NewBufReader(r io.Reader, bufSize int) *BufReader {
	br := &BufReader{
		r:      r,
		ch:     make(chan []byte), // unbuffered
		u64buf: make([]byte, binary.MaxVarintLen64),
	}
	for i := 0; i < len(br.bufs); i++ {
		br.bufs[i] = make([]byte, bufSize)
	}
	go br.fill()
	return br
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
