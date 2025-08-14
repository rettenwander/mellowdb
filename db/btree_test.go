package db_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/rettenwander/mellowdb/db"
	"github.com/rettenwander/mellowdb/io"
)

type NodeReaderMOCK struct {
	nodes       map[io.PageID]db.Node
	MaxNodeSize int

	ReadCounter   int
	WriteCounter  int
	GetNewCounter int
}

func (r *NodeReaderMOCK) ReadNode(id io.PageID) (*db.Node, error) {
	r.ReadCounter++
	node, ok := r.nodes[id]
	if !ok {
		return nil, errors.New("Node not found")
	}

	buf := make([]byte, node.Size())
	node.WriteToBuffer(buf)

	clone := db.NewEmptyNode(node.PageID())
	clone.ReadFromBuffer(buf)
	return clone, nil
}

func (r *NodeReaderMOCK) WriteNode(n *db.Node) error {
	r.WriteCounter++
	r.nodes[n.PageID()] = *n
	return nil
}

func (r *NodeReaderMOCK) GetNewNode() *db.Node {
	r.GetNewCounter++
	id := io.PageID(1)
	for {
		_, ok := r.nodes[id]
		if !ok {
			node := db.NewEmptyNode(id)
			r.nodes[id] = *node
			return node
		}

		id++
	}
}

func (r *NodeReaderMOCK) GetMaxNodeSize() int {
	return r.MaxNodeSize
}

func TestBTreeFind(t *testing.T) {
	reader := &NodeReaderMOCK{
		nodes: make(map[int64]db.Node),
	}

	item1, _ := db.NewItem([]byte("1"), []byte("Value"))
	item2, _ := db.NewItem([]byte("2"), []byte("Value"))
	item3, _ := db.NewItem([]byte("3"), []byte("Value"))
	item4, _ := db.NewItem([]byte("4"), []byte("Value"))
	item5, _ := db.NewItem([]byte("5"), []byte("Value"))
	item6, _ := db.NewItem([]byte("6"), []byte("Value"))
	item7, _ := db.NewItem([]byte("7"), []byte("Value"))

	rootNode := reader.GetNewNode()
	rootNode.AddItem(item4, 0)

	rootNode.AddChild(2, 0)
	rootNode.AddChild(5, 1)

	t.Log(rootNode.Size())

	lnode := reader.GetNewNode()
	lnode.AddItem(item2, 0)
	lnode.AddChild(3, 0)
	lnode.AddChild(4, 1)

	llnode := reader.GetNewNode()
	llnode.AddItem(item1, 0)

	lrnode := reader.GetNewNode()
	lrnode.AddItem(item3, 0)

	rnode := reader.GetNewNode()
	rnode.AddItem(item6, 0)
	rnode.AddChild(6, 0)
	rnode.AddChild(7, 1)

	rlnode := reader.GetNewNode()
	rlnode.AddItem(item5, 0)

	rrnode := reader.GetNewNode()
	rrnode.AddItem(item7, 0)

	reader.WriteNode(rootNode)
	reader.WriteNode(lnode)
	reader.WriteNode(llnode)
	reader.WriteNode(lrnode)
	reader.WriteNode(rnode)
	reader.WriteNode(rlnode)
	reader.WriteNode(rrnode)

	tree := db.NewBTree(reader, 1)

	for key := range 7 {
		if key == 0 {
			continue
		}

		bkey := strconv.Itoa(key)
		_, err := tree.Find([]byte(bkey))
		if err != nil {
			t.Fatalf("Error finding key: %s %v", bkey, err)
		}
	}

	_, err := tree.Find([]byte("Not existing key"))
	if err == nil {
		t.Fatal("Found key not in tree")
	} else if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestInsert(t *testing.T) {
	reader := &NodeReaderMOCK{
		nodes:       make(map[int64]db.Node),
		MaxNodeSize: 60,
	}

	rootNode := reader.GetNewNode()
	reader.nodes[1] = *rootNode
	tree := db.NewBTree(reader, 1)

	numOfItems := 60000

	for i := range numOfItems {
		if i == 0 {
			continue
		}

		key := []byte(strconv.Itoa(i))
		value := append([]byte("Value "), key...)
		item, _ := db.NewItem(key, value)

		if err := tree.Insert(item); err != nil {
			t.Fatalf("Error inserting %d, %v", i, err)
		}

		if _, err := tree.Find(key); err != nil {
			for _, node := range reader.nodes {
				found, _ := node.FindKeyInNode(key)
				if found == true {
					tree.DumpTree(t, tree.Root, "")
					t.Fatalf("Key %s not found: but there", key)
				}
			}
			tree.DumpTree(t, tree.Root, "")
			t.Fatalf("Key %s not found", key)
		}

	}

	for i := range numOfItems {
		if i == 0 {
			continue
		}

		key := []byte(strconv.Itoa(i))
		if _, err := tree.Find(key); err != nil {
			for _, node := range reader.nodes {
				found, _ := node.FindKeyInNode(key)
				if found == true {
					t.Fatalf("Key %s not found: but there", key)
				}
			}
			t.Fatalf("Key %s not found", key)
		}
	}

	t.Logf("WriteCounter: %d, %d\n", reader.WriteCounter, len(reader.nodes))

	if reader.WriteCounter < len(reader.nodes) {
		t.Fatalf("Not all node are written. WriteCounter: %d, %d", reader.WriteCounter, len(reader.nodes))
	}

	for key, node := range reader.nodes {
		if float32(node.Size()) > float32(reader.MaxNodeSize)*db.MaxFillPercent {
			t.Fatalf("A node is do big: %d, %d", node.Size(), key)
		}
	}
}
