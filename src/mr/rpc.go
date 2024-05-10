package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

type RegisterReply struct {
	WorkerId int
}

func MarshalRegisterCall() (interface{}, *RegisterReply, func() int) {

	reply := RegisterReply{}
	return new(interface{}), &reply, func() int {
		return reply.WorkerId
	}
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	WorkerId       int
	TaskType       int
	MapTaskId      int
	ReduceTaskId   int
	Filename       string
	ReportInterval int
}

func MarshalGetTaskCall(workerId int) (*TaskArgs, *TaskReply, func() (error, Task)) {
	reply := TaskReply{}
	return &TaskArgs{WorkerId: workerData.workerId}, &reply, func() (error, Task) {
		var task Task

		// fmt.Printf("MarshalGetTaskCall: reply.TaskType %v\n", reply.TaskType)

		switch reply.TaskType {
		case NO_TASK_TYPE:
			task = nil
		case MAP_TASK_TYPE:
			task = MapTask{
				id: reply.MapTaskId,
				inputSlice: InputSlice{
					filename: reply.Filename,
				},
				reportInterval: reply.ReportInterval,
			}
		case REDUCE_TASK_TYPE:
			task = ReduceTask{}
		default:
			err := fmt.Errorf("invalid task type %v", reply.TaskType)
			return err, nil
		}

		return nil, task
	}
}

type TaskStatusArgs struct {
	WorkerId     int
	TaskType     int
	MapTaskId    int
	ReduceTaskId int
	Progress     int
	Complete     bool
}

type TaskStatusReply struct {
}

func MarshalTaskStatusCall(workerId int, task Task, progress int, complete bool) (*TaskStatusArgs, *TaskStatusReply, func()) {
	args := TaskStatusArgs{
		WorkerId:  workerId,
		TaskType:  task.TaskType(),
		MapTaskId: task.Id(),
		Progress:  progress,
		Complete:  complete,
	}
	reply := TaskStatusReply{}
	return &args, &reply, func() {
		// Placeholder
	}
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
