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

	keys := [][]byte{[]byte("4"), []byte("6"), []byte("9")}
	items := make([]*db.Item, len(keys))

	for i, key := range keys {
		value := append([]byte("Value "), key...)
		items[i], _ = db.NewItem(key, value)
	}

	rootNode := db.NewEmptyNode()
	lnode := db.NewEmptyNode()
	rnode := db.NewEmptyNode()

	rootNode.AddItem(items[1], 0)
	rootNode.AddChild(2)
	rootNode.AddChild(3)

	lnode.AddItem(items[0], 0)
	rnode.AddItem(items[2], 0)

	reader.nodes[1] = rootNode
	reader.nodes[2] = lnode
	reader.nodes[3] = rnode

	tree := db.NewBTree(reader, 1)

	for _, key := range keys {
		_, err := tree.Find(key)
		if err != nil {
			t.Fatalf("Error finding key: %s %v", key, err)
		}
	}

	_, err := tree.Find([]byte("Not existing key"))
	if err == nil {
		t.Fatal("Found key not in tree")
	} else if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("Unexpected error: %v", err)
	}
}
