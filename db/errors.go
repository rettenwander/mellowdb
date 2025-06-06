package db

import (
	"errors"
	"fmt"
)

var (
	ErrKeyTooLong   = errors.New(fmt.Sprintf("Key exceeds maximum allowed length of %d bytes", MaxKeySize))
	ErrValueTooLong = errors.New(fmt.Sprintf("Value exceeds maximum allowed length of %d bytes", MaxValueSize))
)
