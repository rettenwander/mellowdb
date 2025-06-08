package db

import (
	"bytes"
	"encoding/binary"

	"github.com/rettenwander/mellowdb/io"
)

type Node struct {
	pageId   io.PageID
	items    []*Item
	children []io.PageID
}

func NewEmptyNode() *Node {
	return &Node{}
}

func (n *Node) isLeaf() bool {
	return len(n.children) == 0
}

// This func expects the buffer to be large enough for node deserialization.
func (n *Node) WriteToBuffer(buf []byte) {
	lPos := 0
	rPos := len(buf)

	// Write is Leaf and item count to start of buff
	isLeaf := n.isLeaf()
	if isLeaf {
		buf[lPos] = 1
	} else {
		buf[lPos] = 0
	}

	lPos += 1

	binary.LittleEndian.PutUint16(buf[lPos:], uint16(len(n.items)))
	lPos += 2

	for i, item := range n.items {
		if !isLeaf {
			// Write child pointer to Start (lPos)
			binary.LittleEndian.PutUint64(buf[lPos:], uint64(n.children[i]))
			lPos += io.PageIDSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		// Write item offset to start (lPos)
		offset := rPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[lPos:], uint16(offset))
		lPos += 2

		// Write Key and Value to the end of buffer (rPos)
		// Format
		//
		// -------------------------------------------
		// | Key Length | Key | Value Length | Vlaue | rPos
		// -------------------------------------------
		rPos -= vlen
		copy(buf[rPos:], item.value)

		rPos -= 1
		buf[rPos] = byte(vlen)

		rPos -= klen
		copy(buf[rPos:], item.key)

		rPos -= 1
		buf[rPos] = byte(klen)
	}

	if !isLeaf {
		lastChild := n.children[len(n.children)-1]
		binary.LittleEndian.PutUint64(buf[lPos:], uint64(lastChild))
	}

}

// This func expects the buffer to be large enough for node serialization.
func (n *Node) ReadFromBuffer(buf []byte) {
	lPos := 0

	isLeaf := uint8(buf[lPos]) == 1
	lPos += 1

	itemCount := int(binary.LittleEndian.Uint16(buf[lPos:]))
	lPos += 2

	n.items = make([]*Item, 0, itemCount)
	//n.children = make([]io.PageID, itemCount+1)

	for i := 0; i < itemCount; i++ {
		if !isLeaf {
			pageID := io.PageID(binary.LittleEndian.Uint64(buf[lPos:]))
			lPos += io.PageIDSize

			n.children = append(n.children, pageID)
		}

		//Write offset to the fix left side
		offset := binary.LittleEndian.Uint16(buf[lPos:])
		lPos += 2

		// Write Key Value to the right side in this format
		// -------------------------------------------
		// | Value | Value Length | Key | Key Length |
		// -------------------------------------------

		klen := uint16(buf[offset])
		offset += 1

		key := buf[offset : offset+klen]
		offset += uint16(klen)

		vlen := uint16(buf[offset])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += uint16(klen)

		n.items = append(n.items, &Item{key: key, value: value})
	}

	if !isLeaf {
		pageID := io.PageID(binary.LittleEndian.Uint64(buf[lPos:]))
		lPos += io.PageIDSize

		n.children = append(n.children, pageID)
	}
}

func (n *Node) AddItem(i *Item, index int) {
	if len(n.items) == index {
		n.items = append(n.items, i)
		return
	}

	n.items = append(n.items[:index+1], n.items[index:]...)
	n.items[index] = i
}

func (n *Node) AddChild(id io.PageID) error {
	n.children = append(n.children, id)
	return nil
}

// Returns a boolean indicating if the key was found.
// If true, the second return value is the index of the key in the node.
// If false, the second return value is the index of the child node to search next.
func (n *Node) FindKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.items {
		res := bytes.Compare(existingItem.key, key)
		// Keys match
		if res == 0 {
			return true, i
		}

		// The key is not in this node, search child nodes
		if res == 1 {
			return false, i
		}
	}

	// The key is not in this node, search child nodes
	return false, len(n.items)
}
