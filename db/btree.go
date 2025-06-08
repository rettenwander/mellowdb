package db

import (
	"github.com/rettenwander/mellowdb/io"
)

type NodeReader interface {
	ReadNode(id io.PageID) (*Node, error)
}

type BTree struct {
	root io.PageID

	NodeReader
}

func NewBTree(db NodeReader, root io.PageID) *BTree {
	return &BTree{NodeReader: db, root: root}
}

func (t *BTree) Find(key []byte) (*Item, error) {
	if t.root == 0 {
		return nil, ErrNotFound
	}
	rootNode, err := t.ReadNode(t.root)
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
	ancestorsIndexes := make([]int, 0, 2)
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
