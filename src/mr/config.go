package mr

import "time"

var (
	CoordinatorIPAndPort = "127.0.0.1:6824"
	MapTaskPrefix        = "Map"
	ReduceTaskPrefix     = "Reduce"
	MaxProcessTime       = 11 * time.Second
	TmpDirPath           = "./"
)

const (
	StatusReady = iota
	StatusRunning
	StatusFinished
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
