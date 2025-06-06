package io_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rettenwander/mellowdb/io"
)

func TestNewIOEngine(t *testing.T) {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.mellow")

	options := io.EngineOptions{
		PageSize: uint32(os.Getpagesize()),
		FileName: file,
	}
	e, err := io.NewEngine(options)
	if err != nil {
		t.Fatalf("io.Engine - open file failed: %v", err)
	}

	err = e.Close()
	if err != nil {
		t.Fatalf("io.Engine - close file failed: %v", err)
	}
}

func TestIOEngineRW(t *testing.T) {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.mellow")

	options := io.EngineOptions{
		PageSize: uint32(os.Getpagesize()),
		FileName: file,
	}
	e, err := io.NewEngine(options)
	if err != nil {
		t.Fatalf("io.Engine - open file failed: %v", err)
	}
	defer e.Close()

	data := []byte("This is test data")

	pageW := e.AllocateEmptyPage()
	copy(pageW.Data[:len(data)], data)

	err = e.WritePage(pageW)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	pageR, err := e.ReadPage(pageW.GetID())
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	if bytes.Compare(pageR.Data, pageW.Data) != 0 {
		t.Fatalf("The read data is different from the written data.")
	}
}

func TestPersitMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.mellow")

	options := io.EngineOptions{
		PageSize: uint32(os.Getpagesize()),
		FileName: file,
	}
	e, err := io.NewEngine(options)
	if err != nil {
		t.Fatalf("io.Engine - open file failed: %v", err)
	}

	e.Metadata.MaxPageID = 6
	e.Close()

	e, err = io.NewEngine(options)
	if err != nil {
		t.Fatalf("io.Engine - open file failed: %v", err)
	}

	if e.Metadata.MaxPageID != 6 {
		t.Fatal("Metadata is not correct loaded or saved")
	}

	e.Close()
}

func TestFreePage(t *testing.T) {
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.mellow")

	options := io.EngineOptions{
		PageSize: uint32(os.Getpagesize()),
		FileName: file,
	}
	e, err := io.NewEngine(options)
	if err != nil {
		t.Fatalf("io.Engine - open file failed: %v", err)
	}

	firstID := e.GetNextFreePageID()
	secondID := e.GetNextFreePageID()
	if firstID == secondID {
		t.Fatal("The same PageID is used twice")
	}

	e.MarkPageAsFree(firstID)

	thirdID := e.GetNextFreePageID()
	if firstID != thirdID {
		t.Fatal("The Freed Page ID is not used")
	}
}
