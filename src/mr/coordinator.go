package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// const INPUT_SLICE_SIZE_MB = 64
// const INPUT_SLICE_SIZE_BYTES = 64 * 1024

const MINIMUM_NUMBER_OF_WORKERS = 5

const (
	COORDINATOR_INIT_PHASE = iota
	COORDINATOR_MAP_PHASE
	COORDINATOR_REDUCE_PHASE
	COORDINATOR_CLEANUP_PHASE
	COORDINATOR_COMPLETE_PHASE
)

type PhaseData struct {
	numberOfTotalTasks int
}

type Coordinator struct {
	// Phase data
	phase           int
	nReduce         int
	mapPhaseData    PhaseData
	reducePhaseData PhaseData

	// Worker state
	muWorkers sync.Mutex
	workers   map[int]TaskWorker

	// Task tracking
	muToDo       sync.Mutex
	toDo         map[int]Task
	muInProgress sync.Mutex
	inProgress   map[int]Task
	muCompleted  sync.Mutex
	completed    map[int]Task

	// Notification channels
	clientAddresses   []string
	broadcastChannels map[string]chan int
}

type TaskWorker struct {
	workerId int
	state    int
	taskType int
	task     Task
	address  string
}

func (c *Coordinator) AddUpdateWorker(w TaskWorker) {
	c.muWorkers.Lock()
	defer c.muWorkers.Unlock()
	c.workers[w.workerId] = w
}

func (c *Coordinator) GetWorker(workerId int) TaskWorker {
	c.muWorkers.Lock()
	defer c.muWorkers.Unlock()
	return c.workers[workerId]
}

func (c *Coordinator) AddToDoTask(t Task) {
	c.muToDo.Lock()
	defer c.muToDo.Unlock()
	c.toDo[t.Id()] = t
}

func (c *Coordinator) GetToDoTask(id int) (Task, bool) {
	c.muToDo.Lock()
	defer c.muToDo.Unlock()
	t, ok := c.toDo[id]
	return t, ok
}

func (c *Coordinator) RemoveToDoTask(t Task) {
	c.muToDo.Lock()
	defer c.muToDo.Unlock()
	delete(c.toDo, t.Id())
}

func (c *Coordinator) AddInProgressTask(t Task) {
	c.muInProgress.Lock()
	defer c.muInProgress.Unlock()
	c.inProgress[t.Id()] = t
}

func (c *Coordinator) GetInProgressTask(id int) (Task, bool) {
	c.muToDo.Lock()
	defer c.muToDo.Unlock()
	t, ok := c.inProgress[id]
	return t, ok
}

func (c *Coordinator) RemoveInProgressTask(t Task) {
	c.muInProgress.Lock()
	defer c.muInProgress.Unlock()
	delete(c.inProgress, t.Id())
}

func (c *Coordinator) AddCompletedTask(t Task) {
	c.muCompleted.Lock()
	defer c.muCompleted.Unlock()
	c.completed[t.Id()] = t
}

func (c *Coordinator) GetCompletedTask(id int) (Task, bool) {
	c.muToDo.Lock()
	defer c.muToDo.Unlock()
	t, ok := c.completed[id]
	return t, ok
}

func (c *Coordinator) RemoveCompletedTask(t Task) {
	c.muCompleted.Lock()
	defer c.muCompleted.Unlock()
	delete(c.completed, t.Id())
}

func (c *Coordinator) UpdateClientAddresses() {
	c.muWorkers.Lock()
	defer c.muWorkers.Unlock()
	var addresses []string
	for _, w := range c.workers {
		addresses = append(addresses, w.address)
	}
	c.clientAddresses = addresses
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PrintTaskReport() {
	var phaseString string
	var totalTasks int
	switch c.phase {
	case COORDINATOR_INIT_PHASE:
		phaseString = "Phase: Init"
	case COORDINATOR_MAP_PHASE:
		phaseString = "Phase: Map"
		totalTasks = c.mapPhaseData.numberOfTotalTasks
	case COORDINATOR_REDUCE_PHASE:
		phaseString = "Phase: Reduce"
		totalTasks = c.reducePhaseData.numberOfTotalTasks
	case COORDINATOR_CLEANUP_PHASE:
		phaseString = "Phase: Clean up"
	case COORDINATOR_COMPLETE_PHASE:
		phaseString = "Phase: Complete"
	}
	fmt.Printf("%s --- To Do: %d, In Progress: %d, Completed: %d, Total: %d\n",
		phaseString, len(c.toDo), len(c.inProgress), len(c.completed), totalTasks)
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	// Phase restrictions
	switch c.phase {
	case COORDINATOR_INIT_PHASE:
	// case COORDINATOR_MAP_PHASE:
	// case COORDINATOR_REDUCE_PHASE:
	default:
		err := fmt.Errorf("invalid phase %v", c.phase)
		return err
	}

	// Register worker ID
	workerId := len(c.workers)
	c.AddUpdateWorker(TaskWorker{
		workerId: workerId,
		state:    WORKER_STATE_IDLE,
		taskType: NO_TASK_TYPE,
		task:     nil,
		address:  args.ClientAddress,
	})

	// Add client address
	c.clientAddresses = append(c.clientAddresses, args.ClientAddress)

	fmt.Printf("WorkerId %v registered.\n", workerId)

	// Reply
	// fmt.Println("worker id:", workerId)
	reply.WorkerId = workerId
	// fmt.Println("reply worker id:", reply.WorkerId)
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Printf("GetTask (Coordinator): Request from WorkerId %v\n", args.WorkerId)

	// Phase switch
	switch c.phase {
	case COORDINATOR_MAP_PHASE:
	case COORDINATOR_REDUCE_PHASE:
	default:
		err := fmt.Errorf("invalid phase %v", c.phase)
		return err
	}

	// Get task
	i := len(c.toDo) - 1

	// No tasks available
	if i < 0 {
		log.Printf("GetTask (Coordinator): No tasks available.")
		reply.WorkerId = args.WorkerId
		reply.TaskType = NO_TASK_TYPE

		// Update worker state
		c.AddUpdateWorker(TaskWorker{
			workerId: args.WorkerId,
			state:    WORKER_STATE_IDLE,
			taskType: NO_TASK_TYPE,
			task:     nil,
		})
		return nil
	}

	t, ok := c.GetToDoTask(i)
	if !ok {
		// log.Fatalf("error %v", err)
		log.Printf("GetTask (Coordinator): Missing task at index %v", i)
		err := fmt.Errorf("Task tracking error")
		return err
	}
	c.RemoveToDoTask(t)

	// Reply to worker
	reply.WorkerId = args.WorkerId
	reply.TaskType = t.TaskType()
	reply.MapTaskId = t.Id()
	reply.ReportInterval = 5

	// Phase-specific handling
	switch c.phase {
	case COORDINATOR_MAP_PHASE:
		reply.Filename = t.(MapTask).inputSlice.filename
		reply.NReduce = c.reducePhaseData.numberOfTotalTasks
	case COORDINATOR_REDUCE_PHASE:
	}

	// fmt.Printf("GetTask (Coordinator): WorkerId %v, TaskType %v, MapTaskId %v, nReduce %v, Filename %v\n",
	// 	reply.WorkerId, reply.TaskType, reply.MapTaskId, reply.NReduce, reply.Filename)

	// Move task to inProgress
	//t.startTime = time.Now()
	c.AddInProgressTask(t)

	// Update worker state
	c.AddUpdateWorker(TaskWorker{
		workerId: args.WorkerId,
		state:    WORKER_STATE_ACTIVE,
		taskType: t.TaskType(),
		task:     t,
	})

	c.PrintTaskReport()
	return nil
}

func (c *Coordinator) StatusReport(args *StatusReportArgs, reply *StatusReportReply) error {
	// Phase restrictions
	switch c.phase {
	case COORDINATOR_MAP_PHASE:
	case COORDINATOR_REDUCE_PHASE:
	default:
		err := fmt.Errorf("invalid phase %v", c.phase)
		return err
	}

	w := c.GetWorker(args.WorkerId)

	// Task report
	fmt.Printf("StatusReport (Coordinator): WorkerId %v, TaskType %v, MapTaskId %v \n", w.workerId, w.task.TaskType(), w.task.Id())
	fmt.Printf("StatusReport (Coordinator): Progress: %v, Complete: %v \n", args.Progress, args.Complete)

	// Complete
	if args.Complete {
		//t.lastUpdatedTime = time.Now()

		t, ok := c.GetInProgressTask(w.task.Id())
		if !ok {
			log.Printf("StatusReport (Coordinator): Missing task at index %v", w.task.Id())
			err := fmt.Errorf("Task tracking error")
			return err
		}

		// Remove from inProgress
		c.RemoveInProgressTask(t)

		// Add to completed
		c.AddCompletedTask(t)

		c.AddUpdateWorker(TaskWorker{
			workerId: w.workerId,
			state:    WORKER_STATE_IDLE,
			taskType: NO_TASK_TYPE,
			task:     nil,
		})

		c.PrintTaskReport()
	}

	// TODO: Handle stragglers
	// TODO: Manage failures

	return nil
}

func (c *Coordinator) InitMapPhase(files []string) error {
	c.mapPhaseData.numberOfTotalTasks = len(files)
	// Calculate input slices
	fmt.Println("InitMapPhase (Coordinator): Creating tasks...")
	for i, filename := range files {
		//fmt.Printf("id: %d, file %v\n", i, filename)

		t := MapTask{
			id:         i,
			inputSlice: InputSlice{filename: filename},
		}
		c.AddToDoTask(t)

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

	c.PrintTaskReport()
	return nil
}

func (c *Coordinator) InitReducePhase() error {
	fmt.Println("InitMapPhase (Coordinator): Creating tasks...")
	for i := 0; i < c.reducePhaseData.numberOfTotalTasks; i++ {
		t := ReduceTask{
			id: i,
		}
		c.AddToDoTask(t)
	}

	c.PrintTaskReport()
	return nil
}

// Loop that keeps coordinator running until all tasks are complete
func (c *Coordinator) StatusCheck() error {
	for {
		// Check status
		err := c.PhaseCheck()
		if err != nil {
			log.Fatalf("error %v", err)
			return err
		}
		time.Sleep(time.Second)

		if c.phase == COORDINATOR_COMPLETE_PHASE {
			break
		}
	}
	return nil
}

// Check phase status and transitions when conditions are met
func (c *Coordinator) PhaseCheck() error {
	// Phase restrictions
	switch c.phase {
	case COORDINATOR_INIT_PHASE:
		// Wait for workers to register
		if len(c.workers) < MINIMUM_NUMBER_OF_WORKERS {
			fmt.Printf("PhaseCheck (Coordinator): Waiting for workers to register. %v/%v\n", len(c.workers), MINIMUM_NUMBER_OF_WORKERS)
			return nil
		}

		fmt.Println("PhaseCheck (Coordinator): Change to map phase")
		c.phase = COORDINATOR_MAP_PHASE

		// Broadcast tasks are available to workers
		fmt.Println("PhaseCheck (Coordinator): Broadcast tasks available")
		c.broadcastChannels["notification"] <- TASKS_AVAILABLE_NOTIFICATION

		return nil
	case COORDINATOR_MAP_PHASE:
		//fmt.Println("PhaseCheck (Coordinator): Map phase")

		// Check if all map tasks are complete
		// Check ToDo and InProgress
		if c.mapPhaseData.numberOfTotalTasks == len(c.completed) &&
			len(c.toDo) == 0 && len(c.inProgress) == 0 {
			fmt.Printf("PhaseCheck (Coordinator): Map phase complete.\n")
			fmt.Printf("PhaseCheck (Coordinator): Worker status report...\n")

			// Worker status report
			for _, wk := range c.workers {
				fmt.Printf("WorkerId %v, Task Type %d, Worker State %d\n",
					wk.workerId, wk.state, wk.taskType)
			}

			// Change to reduce phase
			fmt.Println("PhaseCheck (Coordinator): Change to reduce phase")
			c.phase = COORDINATOR_REDUCE_PHASE
			c.InitReducePhase()

			// How often should we check for stragglers?
			// How often should we broadcast tasks are available?
			// Update client addresses when clients drop off or come online
			//} else if len(c.toDo) > 0 {

			//for _, w := range(c.workers) {
			//	if w.state == WORKER_STATE_IDLE {
		}

		return nil
	case COORDINATOR_REDUCE_PHASE:
		fmt.Println("PhaseCheck (Coordinator): Reduce phase")

		// Check ToDo and InProgress
		// Check reduce slice completion status

		// Change to complete phase when reduce phase complete
		fmt.Println("PhaseCheck (Coordinator): Change to clean up phase")
		c.phase = COORDINATOR_CLEANUP_PHASE

		return nil
	case COORDINATOR_CLEANUP_PHASE:
		fmt.Println("PhaseCheck (Coordinator): Clean up phase")

		// TODO
		// Check Worker Statuses
		// Handle stragglers
		// Clean up client sockets for workers that are no longer responding

		// Broadcast quit signal to workers
		fmt.Println("PhaseCheck (Coordinator): Broadcast quit signal")
		c.broadcastChannels["notification"] <- QUIT_NOTIFICATION

		// Quit own broadcast
		c.broadcastChannels["quit"] <- QUIT_NOTIFICATION

		// Close channels
		for _, ch := range c.broadcastChannels {
			close(ch)
		}

		CleanUpBroadcastSocket(coordinatorSock())

		// Change to complete phase when clean up phase complete
		fmt.Println("PhaseCheck (Coordinator): Change to complete phase")
		c.phase = COORDINATOR_COMPLETE_PHASE

		return nil
	case COORDINATOR_COMPLETE_PHASE:
		fmt.Println("PhaseCheck (Coordinator): Complete phase")
		return nil
	default:
		err := fmt.Errorf("invalid phase %v", c.phase)
		return err
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	// Deliver to Worker on Register()
	// Instead of relying on same address space

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

	fmt.Println("Checking Done")

	if c.phase == COORDINATOR_COMPLETE_PHASE {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase: COORDINATOR_INIT_PHASE,
		reducePhaseData: PhaseData{
			numberOfTotalTasks: nReduce,
		},
		workers:         make(map[int]TaskWorker),
		toDo:            make(map[int]Task),
		inProgress:      make(map[int]Task),
		completed:       make(map[int]Task),
		clientAddresses: []string{},
	}

	// Start broadcast socket
	// Provide pointer for client address array (which will be updated by coordinator)
	channels, err := CreateBroadcastSocket(&c.clientAddresses)
	if err != nil {
		log.Fatalf("error %v", err)
	}
	c.broadcastChannels = channels

	// Initialize tasks
	c.InitMapPhase(files)

	// Start RPC server
	go c.StatusCheck()
	go c.server()

	return &c
}
