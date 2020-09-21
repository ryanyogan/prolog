package server

import (
	"fmt"
	"sync"
)

// Log represents the records data-structure
type Log struct {
	mu      sync.Mutex
	records []Record
}

// NewLog is a constructor to return a log
func NewLog() *Log {
	return &Log{}
}

// Append takes a record and adds it to the log
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read takes the offset of the log and returns the Record
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

// Record represents a Value and an Offset
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// ErrOffsetNotFound record not found, offset is too high
var ErrOffsetNotFound = fmt.Errorf("offset not found")
