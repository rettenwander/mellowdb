package db

import (
	"bytes"
	"testing"

	"github.com/rettenwander/mellowdb/io"
)

type NodeReader interface {
	ReadNode(id io.PageID) (*Node, error)
	WriteNode(*Node) error
	GetNewNode() *Node
	GetMaxNodeSize() int
}

type BTree struct {
	Root io.PageID

	NodeReader
}

func NewBTree(db NodeReader, root io.PageID) *BTree {
	return &BTree{NodeReader: db, Root: root}
}

func (t *BTree) Find(key []byte) (*Item, error) {
	if t.Root == 0 {
		return nil, ErrNotFound
	}

	rootNode, err := t.ReadNode(t.Root)
	if err != nil {
		return nil, err
	}

	index, node, _, err := t.findKey(rootNode, key, true)
	if err != nil {
		return nil, err
	}

	if index == -1 || node == nil {
		return nil, ErrNotFound
	}

	item := node.items[index]
	return item, nil
}

func (t *BTree) findKey(node *Node, key []byte, exect bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0}
	index, node, err := t.findKeyHelper(node, key, exect, &ancestorsIndexes)
	if err != nil {
		return -1, nil, ancestorsIndexes, err
	}
	return index, node, ancestorsIndexes, nil
}

func (t *BTree) findKeyHelper(node *Node, key []byte, exect bool, ancestorsIndexes *[]int) (int, *Node, error) {
	// Search for the key inside the node
	wasFound, index := node.FindKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	// If we reached a leaf node and the key wasn't found, it means it doesn't exist.
	if node.isLeaf() {
		if exect == true {
			return -1, nil, nil
		}

		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)

	// Else keep searching the tree
	nextChild, err := t.ReadNode(node.children[index])
	if err != nil {
		return -1, nil, err
	}
	return t.findKeyHelper(nextChild, key, exect, ancestorsIndexes)
}

func (t *BTree) Insert(i *Item) error {
	var rootNode *Node
	var err error

	if t.Root == 0 {
		rootNode = t.GetNewNode()
		t.Root = rootNode.pageId

		rootNode.AddItem(i, 0)
		t.WriteNode(rootNode)
		return nil
	}

	rootNode, err = t.ReadNode(t.Root)
	if err != nil {
		return err
	}

	index, node, ancestorsIndexes, err := t.findKey(rootNode, i.key, false)
	if err != nil {
		return err
	}

	if len(node.items) > index && bytes.Compare(node.items[index].key, i.key) == 0 {
		node.items[index] = i
		t.WriteNode(node)
		return nil
	}

	node.AddItem(i, index)

	if float64(node.Size()) <= (float64(t.GetMaxNodeSize()) * MaxFillPercent) {
		t.WriteNode(node)
		return nil
	}

	ancestors := []*Node{rootNode}
	cur := rootNode
	// read down to the parent of the leaf only
	if len(ancestorsIndexes) > 1 {
		for i := 1; i < len(ancestorsIndexes)-1; i++ {
			cur, _ = t.ReadNode(cur.children[ancestorsIndexes[i]])
			ancestors = append(ancestors, cur)
		}
		// now append the actual mutated leaf (don't re-read it)
		ancestors = append(ancestors, node)
	}

	for i := len(ancestors) - 2; i >= 0; i-- {
		parent := ancestors[i]
		child := ancestors[i+1]

		if float64(child.Size()) > (float64(t.GetMaxNodeSize()) * MaxFillPercent) {
			t.splitNode(parent, child, ancestorsIndexes[i+1])
		}
	}

	if float64(rootNode.Size()) > (float64(t.GetMaxNodeSize()) * MaxFillPercent) {
		newRoot := t.GetNewNode()
		newRoot.AddChild(rootNode.pageId, 0)

		t.splitNode(newRoot, rootNode, 0)
		t.Root = newRoot.pageId
	}

	return nil
}

func (t *BTree) getSplitIndex(n *Node) int {
	size := 3
	size += io.PageIDSize

	for i, item := range n.items {
		size += io.PageIDSize
		size += 3
		size += item.Size()

		if float32(size) > (float32(t.GetMaxNodeSize())*MinFillPercent) && i < len(n.items)-1 {
			return i + 1
		}
	}

	return -1
}

func (t *BTree) splitNode(parent *Node, nodeToSplit *Node, childIndexOfNodeToSplit int) {
	splitIndex := t.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.items[splitIndex].Clone()
	newNode := t.GetNewNode()

	if nodeToSplit.isLeaf() {
		newNode.items = nodeToSplit.items[splitIndex+1:]
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode.items = nodeToSplit.items[splitIndex+1:]
		newNode.children = nodeToSplit.children[splitIndex+1:]

		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.children = nodeToSplit.children[:splitIndex+1]
	}

	parent.AddItem(middleItem, childIndexOfNodeToSplit)
	if len(parent.children) == childIndexOfNodeToSplit+1 {
		parent.children = append(parent.children, newNode.pageId)
	} else {
		parent.children = append(parent.children[:childIndexOfNodeToSplit+1], parent.children[childIndexOfNodeToSplit:]...)
		parent.children[childIndexOfNodeToSplit+1] = newNode.pageId
	}

	t.WriteNode(newNode)
	t.WriteNode(nodeToSplit)
	t.WriteNode(parent)
}

func (tr *BTree) DumpTree(t *testing.T, pg io.PageID, indent string) {
	n, err := tr.ReadNode(pg)
	if err != nil {
		t.Fatalf("read %d: %v", pg, err)
	}
	ks := make([]string, len(n.items))
	for i, it := range n.items {
		ks[i] = string(it.key)
	}
	t.Logf("%snode %d keys=%v children=%v", indent, n.PageID(), ks, n.children)
	for _, ch := range n.children {
		tr.DumpTree(t, ch, indent+"  ")
	}
}
