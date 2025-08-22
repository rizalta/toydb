package pager

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Helper to create temp db for testing
func createTempDB(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	return dbPath
}

// Test: New pager
func TestNewPager(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	if pager.GetNumPages() != 0 {
		t.Errorf("expected 0 pages for new pager, got %d", pager.GetNumPages())
	}
}

// Test: Pager with existing file
func TestNewPagerExistingFile(t *testing.T) {
	dbPath := createTempDB(t)

	file, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	data := make([]byte, PageSize*2)
	_, err = file.Write(data)
	if err != nil {
		t.Fatalf("failed to write data to file, %v", err)
	}
	file.Close()

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	if pager.GetNumPages() != 2 {
		t.Errorf("expected 2 pages for existing database, got %d", pager.GetNumPages())
	}
}

// Test: NewPage
func TestNewPage(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	page, err := pager.NewPage()
	if err != nil {
		t.Fatalf("failed to create a new page: %v", err)
	}

	if page.ID != 0 {
		t.Errorf("expected first page id to 0 but got %d", page.ID)
	}

	if pager.GetNumPages() != 1 {
		t.Errorf("expected 1 page after adding a page, got %d", pager.GetNumPages())
	}

	page2, err := pager.NewPage()
	if err != nil {
		t.Fatalf("failed to create a new page: %v", err)
	}

	if page2.ID != 1 {
		t.Errorf("expected first page id to 1 but got %d", page2.ID)
	}

	if pager.GetNumPages() != 2 {
		t.Errorf("expected 2 pages after adding second page, got %d", pager.GetNumPages())
	}
}

func TestReadandWrite(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	page, err := pager.NewPage()
	if err != nil {
		t.Fatalf("failed to create a new page: %v", err)
	}

	testData := []byte("Hello, this is test data.\n")
	copy(page.Data[:], testData)

	err = pager.WritePage(page)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	readPage, err := pager.ReadPage(page.ID)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	if readPage.ID != page.ID {
		t.Errorf("expected page id %d, got %d", page.ID, readPage.ID)
	}

	if !bytes.Equal(readPage.Data[:len(testData)], testData) {
		t.Errorf("page data doesn't match. expected %s, got %s", testData, readPage.Data[:len(testData)])
	}
}

func TestReadNonExistentPage(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	_, err = pager.ReadPage(PageID(1000))
	if err == nil {
		t.Errorf("expecter an error when reading non existent pages, got nil")
	}
}

func TestMultiplePages(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	numPages := 5
	pages := make([]*Page, numPages)
	expectedData := make([][]byte, numPages)

	for i := range numPages {
		page, err := pager.NewPage()
		if err != nil {
			t.Fatalf("failed to create a page %d: %v", i, err)
		}

		testData := fmt.Appendf(nil, "Page %s data", string(rune(i+'0')))
		copy(page.Data[:], testData)

		err = pager.WritePage(page)
		if err != nil {
			t.Fatalf("failed to write page %d: %v", page.ID, err)
		}

		pages[i] = page
		expectedData[i] = testData
	}

	for i := range numPages {
		pageID := pages[i].ID
		readPage, err := pager.ReadPage(pageID)
		if err != nil {
			t.Fatalf("failed to read page %d: %v", pageID, err)
		}

		if readPage.ID != pageID {
			t.Errorf("expected page id %d, got %d", pageID, readPage.ID)
		}

		readData := readPage.Data[:len(expectedData[i])]
		if !bytes.Equal(readData, expectedData[i]) {
			t.Errorf("expected data %s on page %d, got %s", readData, readPage.ID, expectedData[i])
		}
	}
}

func TestPagerClose(t *testing.T) {
	dbPath := createTempDB(t)

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	err = pager.Close()
	if err != nil {
		t.Errorf("failed to close pager: %v", err)
	}

	_, err = pager.NewPage()
	if err == nil {
		t.Error("expected error when creating page after close, got nil")
	}
}
