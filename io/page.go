package io

type PageID = int64

type Page struct {
	id   PageID
	Data []byte
}

func (p *Page) GetID() PageID {
	return p.id
}
