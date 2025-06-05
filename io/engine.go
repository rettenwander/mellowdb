package io

import (
	"os"
)

type EngineOptions struct {
	FileName string
	PageSize int64
}

type Engine struct {
	options EngineOptions

	file *os.File
}

func NewEngine(optoins EngineOptions) (*Engine, error) {
	e := &Engine{options: optoins, file: nil}

	err := e.open()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) open() error {
	if e.file != nil {
		return nil
	}

	file, err := os.OpenFile(e.options.FileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	e.file = file
	return nil
}

func (e *Engine) Close() error {
	if e.file == nil {
		return nil
	}

	return e.file.Close()
}

func (e *Engine) ReadPage(id PageID) (*Page, error) {
	page := e.AllocateEmptyPage()
	page.id = id

	offset := int64(id) * int64(e.options.PageSize)
	_, err := e.file.ReadAt(page.Data, offset)
	if err != nil {
		return nil, err
	}

	return page, nil
}

func (e *Engine) WritePage(page *Page) error {
	offset := int64(page.id) * int64(e.options.PageSize)

	_, err := e.file.WriteAt(page.Data, offset)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) AllocateEmptyPage() *Page {
	return &Page{Data: make([]byte, e.options.PageSize)}
}
