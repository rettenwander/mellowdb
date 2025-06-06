package db_test

import (
	"errors"
	"testing"

	"github.com/rettenwander/mellowdb/db"
	"github.com/rettenwander/mellowdb/io"
)

type NodeReaderMOCK struct {
	nodes map[io.PageID]*db.Node
}

func (r *NodeReaderMOCK) ReadNode(id io.PageID) (*db.Node, error) {
	node, ok := r.nodes[id]
	if !ok {
		return nil, errors.New("Note not found")
	}

	return node, nil
}

func TestBTreeFind(t *testing.T) {
	reader := &NodeReaderMOCK{
		nodes: make(map[int64]*db.Node),
	}

	item1, _ := db.NewItem([]byte("4"), []byte("Value 4"))
	item2, _ := db.NewItem([]byte("6"), []byte("Value 6"))
	item3, _ := db.NewItem([]byte("9"), []byte("Value 9"))

	rootNode := db.NewEmptyNode()
	lnode := db.NewEmptyNode()
	rnode := db.NewEmptyNode()

	rootNode.AddItem(item2)
	rootNode.AddChild(2)
	rootNode.AddChild(3)

	lnode.AddItem(item1)
	rnode.AddItem(item3)

	reader.nodes[1] = rootNode
	reader.nodes[2] = lnode
	reader.nodes[3] = rnode

	tree := db.NewBTree(reader, 1)

	index, nodeFouond, err := tree.FindKey([]byte("6"))
	if err != nil {
		t.Fatalf("Error finding key: %v", err)
	}

	if nodeFouond == nil {
		t.Fatal("Node not found")
	}

	if index == -1 {
		t.Fatal("Item index not found")
	}

	index, nodeFouond, err = tree.FindKey([]byte("9"))
	if err != nil {
		t.Fatalf("Error finding key: %v", err)
	}

	if nodeFouond == nil {
		t.Fatal("Node not found")
	}

	if index == -1 {
		t.Fatal("Item index not found")
	}

	index, nodeFouond, err = tree.FindKey([]byte("4"))
	if err != nil {
		t.Fatalf("Error finding key: %v", err)
	}

	if nodeFouond == nil {
		t.Fatal("Node not found")
	}

	if index == -1 {
		t.Fatal("Item index not found")
	}
}
