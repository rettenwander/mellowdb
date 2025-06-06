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

func (c *BTree) FindKey(key []byte) (int, *Node, error) {
	rootNode, err := c.ReadNode(c.root)
	if err != nil {
		return -1, nil, err
	}

	index, node, err := c.findKeyHelper(rootNode, key)
	if err != nil {
		return -1, nil, err
	}
	return index, node, nil
}

func (c *BTree) findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	// Search for the key inside the node
	wasFound, index := node.FindKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	// If we reached a leaf node and the key wasn't found, it means it doesn't exist.
	if node.isLeaf() {
		return -1, nil, nil
	}

	// Else keep searching the tree
	nextChild, err := c.ReadNode(node.children[index])
	if err != nil {
		return -1, nil, err
	}
	return c.findKeyHelper(nextChild, key)
}
