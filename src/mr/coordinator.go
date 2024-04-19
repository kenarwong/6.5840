package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// const INPUT_SLICE_SIZE_MB = 64
// const INPUT_SLICE_SIZE_BYTES = 64 * 1024

const COORDINATOR_INIT_PHASE = 0
const COORDINATOR_MAP_PHASE = 1
const COORDINATOR_REDUCE_PHASE = 2
const COORDINATOR_IDLE_PHASE = 3

const WORKER_STATE_IDLE = 0
const WORKER_STATE_ACTIVE = 1

type Coordinator struct {
	// Your definitions here.
	phase      int
	workers    map[int]TaskWorker
	toDo       *Stack[Task]
	inProgress *Stack[Task]
	completed  *Stack[Task]
}

type TaskWorker struct {
	state    int
	taskType int
	task     Task
}

// func (c *Coordinator) () {
// }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *interface{}, reply *RegisterReply) error {
	// Phase restrictions

	// Register worker ID
	workerId := len(c.workers) + 1
	c.workers[workerId] = TaskWorker{state: WORKER_STATE_IDLE}

	// Reply
	fmt.Println("worker id:", workerId)
	reply.WorkerId = workerId
	fmt.Println("reply worker id:", reply.WorkerId)
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	var phase int
	// Phase restrictions
	switch c.phase {
	case COORDINATOR_MAP_PHASE:
		phase = MAP_TASK_TYPE
	case COORDINATOR_REDUCE_PHASE:
		phase = REDUCE_TASK_TYPE
	default:
		err := fmt.Errorf("invalid phase %v", c.phase)
		return err
	}

	switch phase {
	case MAP_TASK_TYPE:
		// Get next task
		err, t := c.toDo.Pop()
		if err != nil {
			log.Fatalf("error %v", err)
		}

		fmt.Printf("task %v\n", t.(MapTask).id)

		// Reply to worker
		reply.WorkerId = args.WorkerId
		reply.TaskType = MAP_TASK_TYPE
		reply.MapTaskId = t.(MapTask).id
		reply.Filename = t.(MapTask).inputSlice.filename
		reply.ReportInterval = 5

		// Move task to inProgress
		//t.startTime = time.Now()
		c.inProgress.Push(t)

		// Update worker state
		c.workers[args.WorkerId] = TaskWorker{
			state:    WORKER_STATE_ACTIVE,
			taskType: MAP_TASK_TYPE,
			task:     t,
		}

	case REDUCE_TASK_TYPE:
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false // Return true for mrcoordinator.go to exit

	fmt.Println("Looping...")
	// Check input slice completion status
	// Heartbeats from workers
	// Change to reduce phase when map phase complete

	time.Sleep(5)

	// Check reduce slice completion status
	// Heartbeats from workers
	// Set ret to true when reduce phase complete

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:      COORDINATOR_INIT_PHASE,
		workers:    make(map[int]TaskWorker),
		toDo:       NewStack[Task](),
		inProgress: NewStack[Task](),
	}

	//c.toDo = NewStack[Task]()
	//c.inProgress = NewStack[Task]()

	// Calculate input slices
	for i, filename := range files {

		t := MapTask{
			id:         i,
			inputSlice: InputSlice{filename: filename},
		}
		c.toDo.Push(t)

		//	fmt.Println(filename)
		//	fi, err := os.Stat(filename)
		//	if err != nil {
		//		// Could not obtain stat, handle error
		//		log.Fatalf("error stat %v", filename)
		//	}
		//	fmt.Printf("The file is %d bytes long", fi.Size())

		//	//file, err := os.Open(filename)
		//	//if err != nil {
		//	//	log.Fatalf("cannot open %v", filename)
		//	//}

		//	//bArr := make([]byte, sliceSize)
		//	//for {
		//	//	n, err := file.Seek(head, tail)

		//	//	slice := InputSlice{filename: filename}}

		//	//	if err == io.EOF {
		//	//		break
		//	//	}
		//	//}

	}

	// Start RPC server
	c.phase = COORDINATOR_MAP_PHASE
	c.server()
	return &c
}
