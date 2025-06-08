package io

import "errors"

var (
	ErrPageSizeNotUsed = errors.New("Requeted page size can't be used")

	ErrReadPage      = errors.New("Unable to read page")
	ErrWritePage     = errors.New("Unable to write page")
	ErrInvalidPageID = errors.New("Invalid PageID")
	ErrNilFile       = errors.New("DB File is nil")
)
