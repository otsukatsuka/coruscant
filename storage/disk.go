package storage

import (
	"io"
	"os"
)

type DiskManager struct {
	heapFile   *os.File
	nextPageID uint64
}

func (d *DiskManager) readPageData(pageID PageID) ([]byte, error) {
	offset := int64(PageSize) * int64(pageID.toUint64())
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(d.heapFile)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (d *DiskManager) writePageData(pageID PageID, data []byte) error {
	offset := int64(PageSize) * int64(pageID.toUint64())
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.heapFile.Write(data)
	return err
}

func (d *DiskManager) allocatePage() PageID {
	pageId := PageID(d.nextPageID)
	d.nextPageID++
	return pageId
}

func (d *DiskManager) sync() error {
	err := d.heapFile.Sync()
	if err != nil {
		return err
	}
	return d.heapFile.Close()
}

func open(heapFilePath string) (*DiskManager, error) {
	_, err := os.Stat(heapFilePath)
	if os.IsNotExist(err) {
		return nil, err
	}
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return newDiskManager(heapFile)
}

func newDiskManager(heapFile *os.File) (*DiskManager, error) {
	heapFileInfo, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}
	nextPageID := uint64(heapFileInfo.Size()) / PageSize
	return &DiskManager{
		heapFile,
		nextPageID,
	}, nil
}
