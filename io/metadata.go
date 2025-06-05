package io

import (
	"encoding/binary"
)

type Metadata struct {
	PageSize  uint32
	MaxPageID PageID

	ReleasedPages []PageID
}

func NewMetadata() *Metadata {
	return &Metadata{ReleasedPages: make([]PageID, 0), PageSize: MetadataPageSize}
}

func (m *Metadata) WriteToBuffer(buff []byte) {
	pos := 0

	binary.LittleEndian.PutUint32(buff[pos:], uint32(m.PageSize))
	pos += 4

	binary.LittleEndian.PutUint64(buff[pos:], uint64(m.MaxPageID))
	pos += PageIDSize

	binary.LittleEndian.PutUint16(buff[pos:], uint16(len(m.ReleasedPages)))
	pos += 4

	for _, id := range m.ReleasedPages {
		binary.LittleEndian.PutUint64(buff[pos:], uint64(id))
		pos += PageIDSize
	}

}

func (m *Metadata) ReadFromBuffer(buff []byte) {
	pos := 0

	m.PageSize = uint32(binary.LittleEndian.Uint32(buff[pos:]))
	pos += 4

	m.MaxPageID = int64(binary.LittleEndian.Uint64(buff[pos:]))
	pos += PageIDSize

	releasedPagesLen := uint32(binary.LittleEndian.Uint32(buff[pos:]))
	pos += 4

	m.ReleasedPages = make([]int64, releasedPagesLen)

	for i := uint32(0); i < releasedPagesLen; i++ {
		m.ReleasedPages[i] = int64(binary.LittleEndian.Uint64(buff[pos:]))
		pos += PageIDSize
	}
}
