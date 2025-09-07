// Package pager
package pager

import (
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 4096

type PageID uint32

type Page struct {
	ID   PageID
	Data [PageSize]byte
}

type Pager struct {
	file       *os.File
	numPages   uint32
	freeListID PageID
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("pager: failed opening file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("pager: failed to stat file: %w", err)
	}

	numPages := uint32(stat.Size() / PageSize)

	return &Pager{
		file:     file,
		numPages: numPages,
	}, nil
}

func (p *Pager) Close() error {
	return p.file.Close()
}

func (p *Pager) ReadPage(pageID PageID) (*Page, error) {
	if uint32(pageID) >= p.numPages {
		return nil, fmt.Errorf("pager: page %d does not exist", pageID)
	}

	page := &Page{ID: pageID}

	offset := int64(pageID) * PageSize
	_, err := p.file.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to seek to page: %w", err)
	}

	_, err = p.file.Read(page.Data[:])
	if err != nil {
		return nil, fmt.Errorf("pager: failed to read page: %w", err)
	}

	return page, nil
}

func (p *Pager) WritePage(page *Page) error {
	offset := int64(page.ID) * PageSize
	_, err := p.file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("pager: failed to seek to page: %w", err)
	}

	n, err := p.file.Write(page.Data[:])
	if err != nil {
		return fmt.Errorf("pager: failed to write page: %w", err)
	}
	if n != PageSize {
		return fmt.Errorf("pager: partial write: wrote %d bytes, expected %d bytes", n, PageSize)
	}

	err = p.file.Sync()
	if err != nil {
		return fmt.Errorf("pager: failed to sync page: %w", err)
	}

	return nil
}

func (p *Pager) NewPage() (*Page, error) {
	if p.freeListID != 0 {
		page, err := p.ReadPage(p.freeListID)
		if err != nil {
			return nil, fmt.Errorf("pager: failed to read free list page: %v", err)
		}

		nextID := PageID(binary.LittleEndian.Uint32(page.Data[:]))
		p.freeListID = nextID

		for i := range page.Data {
			page.Data[i] = 0
		}

		return page, nil
	}
	pageID := PageID(p.numPages)
	page := &Page{ID: pageID}

	for i := range page.Data {
		page.Data[i] = 0
	}

	err := p.WritePage(page)
	if err != nil {
		return nil, err
	}
	p.numPages++

	return page, nil
}

func (p *Pager) WriteAtOffset(offset uint64, data []byte) error {
	_, err := p.file.Seek(int64(offset), 0)
	if err != nil {
		return fmt.Errorf("pager: failed to seek to offset: %v", err)
	}

	n, err := p.file.Write(data)
	if err != nil {
		return fmt.Errorf("pager: failed to write at offset: %v", err)
	}
	if n != len(data) {
		return fmt.Errorf("pager: partial write: wrote %d bytes, expected %d bytes", n, len(data))
	}

	return p.file.Sync()
}

func (p *Pager) ReadAtOffset(offset uint64, size int) ([]byte, error) {
	_, err := p.file.Seek(int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to seek to offset: %v", err)
	}

	data := make([]byte, size)
	n, err := p.file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to read at offset: %v", err)
	}
	if n != size {
		return nil, fmt.Errorf("pager: partial read: read %d bytes, expected %d bytes", n, size)
	}

	return data, nil
}

func (p *Pager) GetNumPages() uint32 {
	return p.numPages
}

func (p *Pager) GetSize() (uint64, error) {
	stat, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (p *Pager) GetFreeListID() PageID {
	return p.freeListID
}

func (p *Pager) SetFreeListID(pageID PageID) {
	p.freeListID = pageID
}

func (p *Pager) FreePage(pageID PageID) error {
	page := &Page{
		ID: pageID,
	}

	binary.LittleEndian.PutUint32(page.Data[:], uint32(p.freeListID))

	if err := p.WritePage(page); err != nil {
		return err
	}

	p.freeListID = pageID

	return nil
}
