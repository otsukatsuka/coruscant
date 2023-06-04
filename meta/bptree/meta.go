package bptree

import (
	"github.com/otsukatsuka/coruscant/storage"
	"unsafe"
)

type Header struct {
	RootPageID storage.PageID
}

type Meta[T []byte] struct {
	Header *Header
	unused T
}

func NewMeta[T []byte](bytes T) *Meta[T] {
	header := (*Header)(unsafe.Pointer(&bytes[0]))
	return &Meta[T]{
		Header: header,
		unused: bytes[unsafe.Sizeof(*header):],
	}
}
