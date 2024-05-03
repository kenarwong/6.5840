package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type RegisterReply struct {
	WorkerId int
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	//Task           Task
	WorkerId       int
	TaskType       int
	MapTaskId      int
	ReduceTaskId   int
	Filename       string
	ReportInterval time.Duration
}

type TaskStatusArgs struct {
	WorkerId     int
	TaskType     int
	MapTaskId    int
	ReduceTaskId int
	Complete     bool
	Progress     int
}

type TaskStatusReply struct {
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
