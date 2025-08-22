// Package pager
package pager

import (
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
	file     *os.File
	numPages uint32
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

	_, err = p.file.Write(page.Data[:])
	if err != nil {
		return fmt.Errorf("pager: failed to write page: %w", err)
	}

	err = p.file.Sync()
	if err != nil {
		return fmt.Errorf("pager: failed to sync page: %w", err)
	}

	return nil
}

func (p *Pager) NewPage() (*Page, error) {
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

func (p *Pager) GetNumPages() uint32 {
	return p.numPages
}
