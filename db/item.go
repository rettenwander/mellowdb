package db

type Item struct {
	key   []byte
	value []byte
}

func NewItem(key []byte, value []byte) (*Item, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLong
	} else if len(key) > MaxValueSize {
		return nil, ErrValueTooLong
	}

	return &Item{
		key:   key,
		value: value,
	}, nil
}
