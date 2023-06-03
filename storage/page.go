package storage

import "math"

const (
	PageSize      = 4096
	InvalidPageId = math.MaxUint64
)

type PageID uint64

func (pid PageID) valid() *PageID {
	if pid == InvalidPageId {
		return nil
	}
	return &pid
}

func (pid PageID) toUint64() uint64 {
	return uint64(pid)
}
