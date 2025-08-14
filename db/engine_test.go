package db_test

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rettenwander/mellowdb/db"
)

func TestInsertUsingDB(t *testing.T) {
	tmpDir := t.TempDir()
	fmt.Printf("tmpDir: %v\n", tmpDir)
	file := filepath.Join(tmpDir, "test.mellow")

	dbEngine, err := db.NewDB(file)
	if err != nil {
		t.Fatal(err)
	}

	tree := db.NewBTree(dbEngine, 0)

	for i := range 60000 {
		key := []byte(strconv.Itoa(i))
		value := append([]byte("Value "), key...)
		item, _ := db.NewItem(key, value)

		if err := tree.Insert(item); err != nil {
			t.Fatalf("Error inserting %d, %v", i, err)
		}
	}

	err = dbEngine.Close()
	if err != nil {
		t.Fatal(err)
	}

	dbEngine2, err := db.NewDB(file)
	tree = db.NewBTree(dbEngine2, tree.Root)

	if err != nil {
		t.Fatal(err)
	}

	for i := range 60000 {
		key := []byte(strconv.Itoa(i))
		//t.Logf("key: %s", key)
		if _, err := tree.Find(key); err != nil {
			t.Fatalf("Inserted key not found %d, %v", i, err)
		}
	}

	dbEngine.Close()

}
