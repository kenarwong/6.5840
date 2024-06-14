package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

type RegisterArgs struct {
	// For broadcast
	ClientAddress string
}

type RegisterReply struct {
	WorkerId int
}

func MarshalRegisterCall(clientAddress string) (*RegisterArgs, *RegisterReply, func() int) {

	args := RegisterArgs{
		ClientAddress: clientAddress,
	}
	reply := RegisterReply{}
	return &args, &reply, func() int {
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
	NReduce        int
	Filename       string
	ReportInterval int
}

func MarshalGetTaskCall(workerId int) (*TaskArgs, *TaskReply, func() (error, Task)) {
	reply := TaskReply{}
	return &TaskArgs{WorkerId: workerId}, &reply, func() (error, Task) {
		var task Task

		// fmt.Printf("MarshalGetTaskCall: reply.TaskType %v\n", reply.TaskType)

		switch reply.TaskType {
		case NO_TASK_TYPE:
			task = nil
		case MAP_TASK_TYPE:
			task = MapTask{
				id:      reply.MapTaskId,
				nReduce: reply.NReduce,
				inputSlice: InputSlice{
					filename: reply.Filename,
				},
				reportInterval: reply.ReportInterval,
			}
		case REDUCE_TASK_TYPE:
			task = ReduceTask{
				id:             reply.ReduceTaskId,
				reportInterval: reply.ReportInterval,
			}
		default:
			err := fmt.Errorf("invalid task type %v", reply.TaskType)
			return err, nil
		}

		// fmt.Printf("GetTask: WorkerId %v, TaskType %v, TaskId %v, nReduce %v\n",
		// 	reply.WorkerId, reply.TaskType, task.Id(), reply.NReduce)

		return nil, task
	}
}

type StatusReportArgs struct {
	WorkerId     int
	TaskType     int
	MapTaskId    int
	ReduceTaskId int
	Progress     int
	Complete     bool
}

type StatusReportReply struct {
}

func MarshalStatusReportCall(workerId int, progress int, complete bool) (*StatusReportArgs, *StatusReportReply, func()) {
	args := StatusReportArgs{
		WorkerId: workerId,
		Progress: progress,
		Complete: complete,
	}
	reply := StatusReportReply{}
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		log.Printf("dialing: %v\n", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Printf("error: %v\n", err)
		return false
	}

	return true
}
