package storage

import "github.com/otsukatsuka/coruscant/errors"

type BufferID int

type Buffer struct {
	pageID  PageID
	page    Page
	isDirty bool
}

func NewDefaultBuffer() *Buffer {
	return &Buffer{
		pageID:  0,
		page:    [PageSize]byte{},
		isDirty: false,
	}
}

type Frame struct {
	usageCount int
	buffer     *Buffer
}
type Frames []*Frame

type BufferPool struct {
	buffers      Frames
	nextVictimID BufferID
}

func NewBufferPool(poolSize int) *BufferPool {
	buffers := make(Frames, poolSize)
	for i := 0; i < poolSize; i++ {
		buffer := NewDefaultBuffer()
		frame := &Frame{
			usageCount: 0,
			buffer:     buffer,
		}
		buffers[i] = frame
	}
	return &BufferPool{
		buffers:      buffers,
		nextVictimID: 0,
	}
}

func (pool *BufferPool) Size() int {
	return len(pool.buffers)
}

func (pool *BufferPool) incrementID(bufferID BufferID) BufferID {
	return (bufferID + 1) % BufferID(pool.Size())
}

func (pool *BufferPool) Evict() *BufferID {
	poolSeize := pool.Size()
	consecutivePinned := 0
	victimID := pool.nextVictimID

	for {
		frame := pool.buffers[victimID]
		if frame.usageCount == 0 {
			break
		}
		if frame.buffer.isDirty || frame.buffer.pageID == 0 {
			frame.usageCount--
			consecutivePinned = 0
		} else {
			consecutivePinned++
			if consecutivePinned >= poolSeize {
				return nil
			}
		}
		pool.nextVictimID = pool.incrementID(pool.nextVictimID)
		victimID = pool.nextVictimID
	}

	return &victimID
}

func (pool *BufferPool) GetBuffer(bufferID BufferID) *Frame {
	return pool.buffers[bufferID]
}

type PageTable map[PageID]BufferID

type BufferPoolManager struct {
	disk      *DiskManager
	pool      *BufferPool
	pageTable PageTable
}

func NewBufferPoolManager(disk *DiskManager, pool *BufferPool) *BufferPoolManager {
	pageTable := make(PageTable)
	return &BufferPoolManager{
		disk:      disk,
		pool:      pool,
		pageTable: pageTable,
	}
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Buffer, error) {
	bufferID, ok := bpm.pageTable[pageID]
	if ok {
		frame := bpm.pool.GetBuffer(bufferID)
		frame.usageCount++
		return frame.buffer, nil
	}
	victimID := bpm.pool.Evict()
	if victimID == nil {
		return nil, errors.NewBufferError("no free buffer available in buffer pool")
	}
	frame := bpm.pool.GetBuffer(*victimID)
	evictPageID := frame.buffer.pageID
	if frame.buffer.isDirty {
		data, err := bpm.disk.readPageData(pageID)
		copy(frame.buffer.page[:], data)
		if err != nil {
			return nil, err
		}
	}
	frame.usageCount = 1
	bpm.pageTable[evictPageID] = 0
	bpm.pageTable[pageID] = *victimID
	return frame.buffer, nil
}

func (bpm *BufferPoolManager) CreatePage() (*Buffer, error) {
	victimId := bpm.pool.Evict()
	if victimId == nil {
		return nil, errors.NewBufferError("no free buffer available in buffer pool")
	}
	frame := bpm.pool.GetBuffer(*victimId)
	evictPageId := frame.buffer.pageID
	pageId := bpm.disk.allocatePage()
	frame.buffer.pageID = pageId
	frame.buffer.isDirty = true
	frame.usageCount = 1
	bpm.pageTable[evictPageId] = 0
	bpm.pageTable[pageId] = *victimId
	return frame.buffer, nil
}

func (bpm *BufferPoolManager) Flush() error {
	for pageId, bufferId := range bpm.pageTable {
		frame := bpm.pool.GetBuffer(bufferId)
		buffer := frame.buffer
		if buffer.isDirty {
			if err := bpm.disk.writePageData(pageId, buffer.page[:]); err != nil {
				return errors.Wrap(err)
			}
			buffer.isDirty = false
		}
	}
	err := bpm.disk.sync()
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}
