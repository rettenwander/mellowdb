package db_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/rettenwander/mellowdb/db"
)

func TestNodeRW(t *testing.T) {
	buf := make([]byte, os.Getpagesize())
	item1, _ := db.NewItem([]byte("Key1"), []byte("Value 1"))
	item2, _ := db.NewItem([]byte("Key2"), []byte("Value 2"))

	node := db.NewEmptyNode()
	node.AddItem(item1)
	node.AddItem(item2)

	node.WriteToBuffer(buf)

	node2 := db.NewEmptyNode()
	node2.ReadFromBuffer(buf)

	if !reflect.DeepEqual(node, node2) {
		t.Fatal("Node is not equal after RW")
	}
}

func TestFindKeyInNode(t *testing.T) {
	item1, _ := db.NewItem([]byte("Key1"), []byte("Value 1"))
	item2, _ := db.NewItem([]byte("Key2"), []byte("Value 2"))

	node := db.NewEmptyNode()
	node.AddItem(item1)
	node.AddItem(item2)

	found, index := node.FindKeyInNode([]byte("Key2"))
	if found == false && index != -1 {
		t.Fatal("key not found")
	}
}
