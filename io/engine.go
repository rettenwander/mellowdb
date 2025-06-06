package io

import (
	"os"
)

type EngineOptions struct {
	FileName string
	PageSize uint32
}

type Engine struct {
	Metadata

	file *os.File
}

func NewEngine(optoins EngineOptions) (*Engine, error) {
	e := &Engine{Metadata: *NewMetadata(), file: nil}

	err := e.open(optoins)
	if err != nil {
		return nil, err
	}

	return e, nil
}

// If the ErrPageSizeNotUsed error is returned, the engine is still operational.
func (e *Engine) open(options EngineOptions) error {
	if e.file != nil {
		return nil
	}

	if _, err := os.Stat(options.FileName); err == nil {
		e.file, err = os.OpenFile(options.FileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}

		metadataPage, err := e.ReadPage(PageID(0))
		if err != nil {
			return err
		}

		e.Metadata.ReadFromBuffer(metadataPage.Data)

		if e.PageSize != options.PageSize {
			return ErrPageSizeNotUsed
		}
	} else {
		e.file, err = os.OpenFile(options.FileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}

		e.Metadata.PageSize = options.PageSize
	}

	return nil
}

func (e *Engine) Close() error {
	if e.file == nil {
		return nil
	}

	metadataPage := e.AllocateEmptyPage()
	metadataPage.id = 0

	e.Metadata.PageSize = MetadataPageSize
	e.Metadata.WriteToBuffer(metadataPage.Data)

	err := e.WritePage(metadataPage)
	if err != nil {
		e.file.Close()
		return err
	}

	return e.file.Close()
}

func (e *Engine) ReadPage(id PageID) (*Page, error) {
	page := e.AllocateEmptyPage()
	page.id = id

	offset := int64(id) * int64(e.PageSize)
	_, err := e.file.ReadAt(page.Data, offset)
	if err != nil {
		return nil, err
	}

	return page, nil
}

func (e *Engine) WritePage(page *Page) error {
	offset := int64(page.id) * int64(e.PageSize)

	_, err := e.file.WriteAt(page.Data, offset)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) AllocateEmptyPage() *Page {
	return &Page{Data: make([]byte, e.PageSize)}
}

func (e *Engine) AllocateEmptyPageWithFreeID() *Page {
	return &Page{Data: make([]byte, e.PageSize), id: e.GetNextFreePageID()}
}

func (e *Engine) GetNextFreePageID() PageID {
	if len(e.ReleasedPages) == 0 {
		e.MaxPageID += 1
		return e.MaxPageID
	}

	pageID := e.ReleasedPages[len(e.ReleasedPages)-1]
	e.ReleasedPages = e.ReleasedPages[:len(e.ReleasedPages)-1]

	return pageID
}

func (e *Engine) MarkPageAsFree(id PageID) {
	if id > e.MaxPageID {
		return
	}

	e.ReleasedPages = append(e.ReleasedPages, id)
}
