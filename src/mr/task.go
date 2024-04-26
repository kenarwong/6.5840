package mr

import (
	"time"
)

const MAP_TASK_TYPE = 1
const REDUCE_TASK_TYPE = 2

type Task interface {
	TaskType() int
}

type Status struct {
	description string
}

type InputSlice struct {
	filename string
	// start    int
	// end      int
}

type MapTask struct {
	id              int
	inputSlice      InputSlice
	startTime       time.Time
	lastUpdatedTime time.Time
	endTime         time.Time
	completed       bool
}

func (t MapTask) TaskType() int {
	return MAP_TASK_TYPE
}

type ReduceTask struct {
	id              int
	filename        string
	outputSlice     int
	startTime       time.Time
	lastUpdatedTime time.Time
	endTime         time.Time
	completed       bool
}

func (t ReduceTask) TaskType() int {
	return REDUCE_TASK_TYPE
}
