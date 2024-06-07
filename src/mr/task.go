package mr

import (
	"time"
)

const (
	NO_TASK_TYPE = iota
	MAP_TASK_TYPE
	REDUCE_TASK_TYPE
)

const WORKER_STATE_IDLE = 0
const WORKER_STATE_ACTIVE = 1
const WORKER_STATE_FAILED = -1

type Task interface {
	Id() int
	TaskType() int
	GetReportInterval() int
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
	reportInterval  int
	completed       bool
}

func (t MapTask) Id() int {
	return t.id
}

func (t MapTask) TaskType() int {
	return MAP_TASK_TYPE
}

func (t MapTask) GetReportInterval() int {
	return t.reportInterval
}

type ReduceTask struct {
	id              int
	filename        string
	outputSlice     int
	startTime       time.Time
	lastUpdatedTime time.Time
	endTime         time.Time
	reportInterval  int
	completed       bool
}

func (t ReduceTask) Id() int {
	return t.id
}

func (t ReduceTask) TaskType() int {
	return REDUCE_TASK_TYPE
}

func (t ReduceTask) GetReportInterval() int {
	return t.reportInterval
}
