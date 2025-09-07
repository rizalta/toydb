// Package pager
package pager

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	PageSize     = 4096
	MaxCacheSize = 128
	SyncPeriod   = 10 * time.Second
)

var ErrPagerClosed = errors.New("pager: operations on a closed pager")

type PageID uint32

type Page struct {
	ID   PageID
	Data [PageSize]byte
}

type Pager struct {
	file       *os.File
	numPages   uint32
	freeListID PageID
	cache      map[PageID]*list.Element
	lruList    *list.List
	mu         sync.Mutex
	isClosed   bool
	done       chan struct{}
	wg         sync.WaitGroup
}

type cacheEntry struct {
	page    *Page
	isDirty bool
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

	p := &Pager{
		file:       file,
		numPages:   numPages,
		cache:      make(map[PageID]*list.Element),
		freeListID: 0,
		lruList:    list.New(),
		mu:         sync.Mutex{},
		isClosed:   false,
		done:       make(chan struct{}),
	}

	p.wg.Add(1)
	go p.startPeriodicSync()

	return p, nil
}

func (p *Pager) readFromDisk(pageID PageID) (*Page, error) {
	page := &Page{ID: pageID}
	offset := int64(pageID) * PageSize

	_, err := p.file.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to seek to page: %w", err)
	}

	_, err = p.file.Read(page.Data[:])
	return page, err
}

func (p *Pager) writeToDisk(page *Page) error {
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

	return nil
}

func (p *Pager) evict() error {
	elem := p.lruList.Back()
	if elem == nil {
		return nil
	}

	entry := elem.Value.(*cacheEntry)
	if entry.isDirty {
		if err := p.writeToDisk(entry.page); err != nil {
			return err
		}
	}

	p.lruList.Remove(elem)
	delete(p.cache, entry.page.ID)

	return nil
}

func (p *Pager) ReadPage(pageID PageID) (*Page, error) {
	if p.isClosed {
		return nil, ErrPagerClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, found := p.cache[pageID]; found {
		p.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).page, nil
	}

	if uint32(pageID) >= p.numPages {
		return nil, fmt.Errorf("pager: page %d does not exist", pageID)
	}

	page, err := p.readFromDisk(pageID)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to read page: %w", err)
	}

	entry := &cacheEntry{page: page, isDirty: false}
	elem := p.lruList.PushFront(entry)
	p.cache[page.ID] = elem

	if p.lruList.Len() > MaxCacheSize {
		if err := p.evict(); err != nil {
			return nil, err
		}
	}

	return page, nil
}

func (p *Pager) WritePage(page *Page) error {
	if p.isClosed {
		return ErrPagerClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	elem, found := p.cache[page.ID]
	var entry *cacheEntry
	if !found {
		entry = &cacheEntry{
			page:    page,
			isDirty: true,
		}
		elem = p.lruList.PushFront(entry)
		p.cache[page.ID] = elem
	} else {
		elem.Value.(*cacheEntry).page = page
		p.lruList.MoveToFront(elem)
	}
	elem.Value.(*cacheEntry).isDirty = true

	if p.lruList.Len() > MaxCacheSize {
		if err := p.evict(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) NewPage() (*Page, error) {
	if p.isClosed {
		return nil, ErrPagerClosed
	}

	if p.freeListID != 0 {
		page, err := p.ReadPage(p.freeListID)
		if err != nil {
			return nil, fmt.Errorf("pager: failed to read free list page: %w", err)
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
		return fmt.Errorf("pager: failed to seek to offset: %w", err)
	}

	n, err := p.file.Write(data)
	if err != nil {
		return fmt.Errorf("pager: failed to write at offset: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("pager: partial write: wrote %d bytes, expected %d bytes", n, len(data))
	}

	return p.file.Sync()
}

func (p *Pager) ReadAtOffset(offset uint64, size int) ([]byte, error) {
	_, err := p.file.Seek(int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to seek to offset: %w", err)
	}

	data := make([]byte, size)
	n, err := p.file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("pager: failed to read at offset: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("pager: partial read: read %d bytes, expected %d bytes", n, size)
	}

	return data, nil
}

func (p *Pager) GetNumPages() uint32 {
	if p.isClosed {
		return 0
	}
	return p.numPages
}

func (p *Pager) GetSize() (uint64, error) {
	if p.isClosed {
		return 0, ErrPagerClosed
	}

	stat, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (p *Pager) GetFreeListID() PageID {
	if p.isClosed {
		return 0
	}

	return p.freeListID
}

func (p *Pager) SetFreeListID(pageID PageID) {
	if !p.isClosed {
		p.freeListID = pageID
	}
}

func (p *Pager) FreePage(pageID PageID) error {
	if p.isClosed {
		return ErrPagerClosed
	}

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

func (p *Pager) Flush() error {
	if p.isClosed {
		return ErrPagerClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, elem := range p.cache {
		entry := elem.Value.(*cacheEntry)
		if entry.isDirty {
			if err := p.writeToDisk(entry.page); err != nil {
				log.Printf("ERROR: failed to write dirty page %d: %v", entry.page.ID, err)
				continue
			}
			entry.isDirty = false
		}
	}

	return p.file.Sync()
}

func (p *Pager) startPeriodicSync() {
	defer p.wg.Done()

	ticker := time.NewTicker(SyncPeriod)

	for {
		select {
		case <-ticker.C:
			if err := p.Flush(); err != nil {
				log.Printf("pager: periodic sync failed: %v", err)
			}
		case <-p.done:
			ticker.Stop()
			return
		}
	}
}

func (p *Pager) Close() error {
	if p.isClosed {
		return nil
	}

	close(p.done)
	p.wg.Wait()

	if err := p.Flush(); err != nil {
		return err
	}

	p.isClosed = true
	return p.file.Close()
}
