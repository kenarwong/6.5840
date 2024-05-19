package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"os"
	"sort"
	"time"
)

type WorkerData struct {
	workerId int
	state    int
	task     Task
	taskType int
	progress int
	complete bool

	// Notification channels
	notification chan int
	report       <-chan time.Time
	quit         chan bool

	// Map and Reduce functions
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

type WorkerProcess struct {
	workerData WorkerData
}

const outputDir = "mr-out"
const tmpDir = "mr-tmp"
const tmpPrefix = "mrtmp."

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *WorkerProcess) WorkerLoop() {

	for {
		select {

		// task notification arrives on channel
		// GetTask()
		// perform task if not already active or failed
		// set active status
		// non-blocking Execute, so we can continue producing heartbeats
		// defer set non-active status

		case <-w.workerData.notification:
			fmt.Printf("WorkerLoop: WorkerId %v: Notification received.\n", w.workerData.workerId)

			// Worker gets task from Coordinator
			err := w.GetTask()
			if err != nil {
				w.workerData.state = WORKER_STATE_FAILED
			}

			// If task exists
			// If worker is not already active or failed
			if w.workerData.task != nil &&
				w.workerData.state != WORKER_STATE_ACTIVE &&
				w.workerData.state != WORKER_STATE_FAILED {

				// Task in goroutine
				// Task and Worker data should never be concurrently modified
				go func() {
					// Set active
					w.workerData.state = WORKER_STATE_ACTIVE

					// Update channel with new ticker
					ticker := time.NewTicker(time.Duration(w.workerData.task.GetReportInterval()) * time.Millisecond)
					w.workerData.report = ticker.C

					// To be performed on completion
					defer func() {
						w.workerData.state = WORKER_STATE_IDLE
						w.workerData.report = nil
						//fmt.Printf("WorkerLoop: WorkerId %v: Completion report\n", w.workerData.workerId)
						w.StatusReport() // Force report on completion

						// ***TEMP ABORT***
						// Replace with coordinator notification
						w.workerData.quit <- true
					}()

					err := w.Execute()
					if err != nil {
						w.workerData.state = WORKER_STATE_FAILED
						w.workerData.report = nil
					}
				}()
			}
		case <-w.workerData.report:
			if w.workerData.state == WORKER_STATE_ACTIVE {
				// TODO: This still sometimes reports a complete status before the task is actually complete
				// TODO: May need mutex on complete status, to report complete = False even if progress is at 100%
				// TODO: Only completion report should send complete = true

				//fmt.Printf("WorkerLoop: WorkerId %v: Active report\n", w.workerData.workerId)
				w.StatusReport()
			}
		case <-w.workerData.quit:
			fmt.Printf("WorkerLoop: WorkerId %v: Quit\n", w.workerData.workerId)
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := WorkerProcess{
		workerData: WorkerData{
			state:        WORKER_STATE_IDLE,
			mapf:         mapf,
			reducef:      reducef,
			notification: make(chan int),
			quit:         make(chan bool),
		},
	}

	// Worker is assigned an ID
	err := worker.Register()
	if err != nil {
		return
	}

	// Goroutine: worker server to receive notifications from coordinator
	// Tasks notification
	// Task notification channel
	// Quit signal
	// Quit channel
	// Remove TEMP

	go func() {
		// ***TEMP TRIGGER***
		// Replace with coordinator notification
		time.Sleep(5 * time.Second)
		worker.workerData.notification <- 1
	}()

	// Start Worker Loop
	worker.WorkerLoop()
}

func (w *WorkerProcess) Register() (err error) {

	args, replyPtr, unmarshal := MarshalRegisterCall()
	ok := call("Coordinator.Register", args, replyPtr)
	if ok {
		w.workerData.workerId = unmarshal()
		fmt.Printf("Register: WorkerId %v registered.\n", w.workerData.workerId)

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("Register failed")
		return err
	}
}

func (w *WorkerProcess) GetTask() (err error) {
	fmt.Printf("GetTask: WorkerId %v.\n", w.workerData.workerId)

	argsPtr, replyPtr, unmarshal := MarshalGetTaskCall(w.workerData.workerId)
	ok := call("Coordinator.GetTask", argsPtr, replyPtr)
	if ok {
		err, w.workerData.task = unmarshal()
		if err != nil {
			log.Fatalf("GetTask (Worker): unmarshal failed")
		}

		// If new task is available
		if w.workerData.task != nil {
			w.workerData.taskType = w.workerData.task.TaskType()
			w.workerData.progress = 0     // Reset progress
			w.workerData.complete = false // Reset complete
		} else {
			w.workerData.taskType = NO_TASK_TYPE
			fmt.Printf("GetTask (WorkerId %v): No tasks available.\n", w.workerData.workerId)
		}

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("GetTask failed")
		return err
	}
}

func (w *WorkerProcess) Execute() (err error) {
	//fmt.Printf("Execute: WorkerId %v.\n", w.workerData.workerId)
	// If map task, Worker calls map
	// If reduce task, Worker calls reduce

	switch w.workerData.taskType {
	case MAP_TASK_TYPE:
		filename := w.workerData.task.(MapTask).inputSlice.filename
		content := Read(filename)

		intermediate := w.workerData.mapf(filename, string(content))

		//intermediate := []KeyValue{}
		//intermediate = append(intermediate, kva...)

		sort.Sort(ByKey(intermediate))

		// fmt.Printf("task id: %d\n", task.(MapTask).id)

		// Make temporary intermediate data directory
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			os.Mkdir(tmpDir, os.ModeDir|0755)
		}
		//else if !os.IsExist(err) {
		//	log.Fatalf("cannot make temporary intermediate data directory: %v", tmpDir)
		//}

		// Loop through all key/value pairs
		i := 0
		for i < len(intermediate) {
			key := intermediate[i].Key

			// fmt.Printf("key: %v\n", key)
			// fmt.Printf("ihash: %v\n", ihash(key))

			oname := fmt.Sprintf(tmpPrefix+"%v-%v", w.workerData.task.Id(), ihash(key))
			filename := tmpDir + "/" + oname
			ofile, err := os.Create(filename)
			if err != nil {
				log.Fatalf("cannot create: %v", filename)
			}

			enc := json.NewEncoder(ofile)

			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}

			// fmt.Printf("j: %v\n", j)

			for k := i; k < j; k++ {
				// fmt.Printf("intermediate[k].Key: %v, value: %v\n", intermediate[k].Key, intermediate[k].Value)
				err := enc.Encode(&intermediate[k])
				if err != nil {
					log.Fatalf("cannot write to %v", oname)
				}
			}

			i = j

			// Update progress
			w.workerData.progress = int(math.Round(float64((i * 100) / len(intermediate))))

			// *** TEMP FAKE DELAY ***
			// To demonstrate progress
			time.Sleep(1 * time.Millisecond)
		}
		w.workerData.complete = true // Officially mark as complete (as opposed to regular reporting)
		fmt.Printf("Execute (Worker): WorkerId: %v, TaskId: %v, Complete: %v\n",
			w.workerData.workerId, w.workerData.task.Id(), w.workerData.complete)

	case REDUCE_TASK_TYPE:
		//content := Read(task.(MapTask).inputSlice.filename)
		//w.workerData.reducef(task.(MapTask).inputSlice.filename, content)
	}

	return nil
}

func (w *WorkerProcess) StatusReport() {
	//fmt.Printf("StatusReport: WorkerId: %v, TaskId: %v, Progress: %v, Complete: %t\n",
	//	w.workerData.workerId, workerData.task.Id(), workerData.progress, workerData.complete)

	argsPtr, replyPtr, _ := MarshalStatusReportCall(w.workerData.workerId, w.workerData.task, w.workerData.progress, w.workerData.complete)

	ok := call("Coordinator.StatusReport", argsPtr, replyPtr)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
