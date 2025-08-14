package db

import (
	"os"

	"github.com/rettenwander/mellowdb/io"
)

type DB struct {
	io *io.Engine
}

func NewDB(fileName string) (*DB, error) {
	options := io.EngineOptions{
		PageSize: uint32(os.Getpagesize()),
		FileName: fileName,
	}

	ioEngine, err := io.NewEngine(options)
	if err != nil {
		return nil, err
	}

	return &DB{io: ioEngine}, nil
}

func (e *DB) Close() error {
	return e.io.Close()
}

func (e *DB) ReadNode(id io.PageID) (*Node, error) {
	page, err := e.io.ReadPage(id)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode(id)
	node.ReadFromBuffer(page.Data)

	return node, nil
}

func (e *DB) WriteNode(n *Node) error {
	page := e.io.AllocateEmptyPage(n.pageId)
	n.WriteToBuffer(page.Data)

	return e.io.WritePage(page)
}

func (e *DB) GetNewNode() *Node {
	return NewEmptyNode(e.io.GetNextFreePageID())
}

func (e *DB) GetMaxNodeSize() int {
	return int(e.io.PageSize)
}

func (e *DB) GetColleactions() {}
