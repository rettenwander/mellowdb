package db

type Item struct {
	key   []byte
	value []byte
}

func NewItem(key []byte, value []byte) (*Item, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLong
	} else if len(value) > MaxValueSize {
		return nil, ErrValueTooLong
	}

	return &Item{
		key:   key,
		value: value,
	}, nil
}

func (i *Item) Size() int {
	size := 2
	size += len(i.key)
	size += len(i.value)
	return size
}

func (i *Item) Clone() *Item {
	newKey := append([]byte(nil), i.key...)
	newValue := append([]byte(nil), i.value...)
	return &Item{key: newKey, value: newValue}
}
