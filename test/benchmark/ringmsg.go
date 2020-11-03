package main

import (
	"hash"
	"hash/crc32"
)

type ringMsg struct {
	crc  hash.Hash32
	sum  uint32
	buf  []byte
	size int
	pos  int
}

func newRingMsg(size int) *ringMsg {
	return &ringMsg{
		crc:  crc32.NewIEEE(),
		buf:  make([]byte, size),
		size: size,
	}
}

func (r *ringMsg) next() []byte {
	pos := r.pos % r.size
	b := r.buf[pos]
	_, _ = r.crc.Write([]byte{b})
	sum := r.crc.Sum(nil)
	r.buf[pos] = sum[0]
	r.buf[(pos+1)%r.size] = sum[1]
	r.buf[(pos+2)%r.size] = sum[2]
	r.buf[(pos+3)%r.size] = sum[3]
	r.pos++
	return r.buf
}
