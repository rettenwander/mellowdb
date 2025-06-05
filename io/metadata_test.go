package io_test

import (
	"reflect"
	"testing"

	"github.com/rettenwander/mellowdb/io"
)

func TestMetatadaRW(t *testing.T) {
	data := make([]byte, 400)

	metadataW := io.NewMetadata()

	metadataW.PageSize = 10
	metadataW.MaxPageID = 1
	metadataW.ReleasedPages = []io.PageID{1, 4, 7}
	metadataW.WriteToBuffer(data)

	metadataR := io.NewMetadata()
	metadataR.ReadFromBuffer(data)

	if !reflect.DeepEqual(metadataW, metadataR) {
		t.Fatalf("Metadata: %d, %d \n% x", metadataW.PageSize, metadataR.PageSize, data)
	}

}
