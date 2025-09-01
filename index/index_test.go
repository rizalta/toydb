package index

import (
	"fmt"

	"github.com/rizalta/toydb/pager"
)

type MockPager struct {
	pages    map[pager.PageID][4096]byte
	numPages uint32
	closed   bool
}

func (mp *MockPager) NewPage() (*pager.Page, error) {
	p := &pager.Page{}
	p.ID = pager.PageID(mp.numPages)
	mp.numPages++
	return p, nil
}

func (mp *MockPager) ReadPage(pageID pager.PageID) (*pager.Page, error) {
	if p, ok := mp.pages[pageID]; ok {
		return &pager.Page{ID: pageID, Data: p}, nil
	} else {
		return nil, fmt.Errorf("invalid page id")
	}
}

func (mp *MockPager) WritePage(page *pager.Page) error {
	mp.pages[page.ID] = page.Data
	return nil
}

func (mp *MockPager) GetNumPages() uint32 {
	return mp.numPages
}

func (mp *MockPager) Close() error {
	mp.closed = true
	return nil
}
