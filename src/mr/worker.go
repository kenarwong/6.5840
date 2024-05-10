package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/rpc"
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

var workerData WorkerData

const outputDir = "mr-out"
const tmpDir = "mr-tmp"
const tmpPrefix = "mrtmp."

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func WorkerLoop() {

	for {
		select {

		// task notification arrives on channel
		// GetTask()
		// perform task if not already active or failed
		// set active status
		// non-blocking Execute, so we can continue producing heartbeats
		// defer set non-active status

		case <-workerData.notification:
			fmt.Printf("WorkerLoop: WorkerId %v: Notification received.\n", workerData.workerId)

			// Worker gets task from Coordinator
			err := GetTask()
			if err != nil {
				workerData.state = WORKER_STATE_FAILED
			}

			// If task exists
			// If worker is not already active or failed
			if workerData.task != nil &&
				workerData.state != WORKER_STATE_ACTIVE &&
				workerData.state != WORKER_STATE_FAILED {

				// Task in goroutine
				// Task and Worker data should never be concurrently modified
				go func() {
					// Set active
					workerData.state = WORKER_STATE_ACTIVE

					// Update channel with new ticker
					ticker := time.NewTicker(time.Duration(workerData.task.GetReportInterval()) * time.Millisecond)
					workerData.report = ticker.C

					// To be performed on completion
					defer func() {
						workerData.state = WORKER_STATE_IDLE
						workerData.report = nil
						ReportStatus() // Force report on completion

						// ***TEMP ABORT***
						workerData.quit <- true
					}()

					err := Execute()
					if err != nil {
						workerData.state = WORKER_STATE_FAILED
						workerData.report = nil
					}
				}()
			}
		case <-workerData.report:
			if workerData.state == WORKER_STATE_ACTIVE {
				//fmt.Printf("WorkerLoop: WorkerId %v: Active report\n", workerData.workerId)
				ReportStatus()
			}
		case <-workerData.quit:
			fmt.Printf("WorkerLoop: WorkerId %v: Quit\n", workerData.workerId)
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerData = WorkerData{
		state:        WORKER_STATE_IDLE,
		mapf:         mapf,
		reducef:      reducef,
		notification: make(chan int),
		quit:         make(chan bool),
	}

	// Worker is assigned an ID
	err := Register()
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
		time.Sleep(5 * time.Second)
		workerData.notification <- 1
	}()

	// Start Worker Loop
	WorkerLoop()
}

func Read(filename string) string {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func Register() (err error) {

	args, replyPtr, unmarshal := MarshalRegisterCall()
	ok := call("Coordinator.Register", args, replyPtr)
	if ok {
		workerData.workerId = unmarshal()
		fmt.Printf("Register: WorkerId %v registered.\n", workerData.workerId)

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("Register failed")
		return err
	}
}

func GetTask() (err error) {
	fmt.Printf("GetTask: WorkerId %v.\n", workerData.workerId)

	argsPtr, replyPtr, unmarshal := MarshalGetTaskCall(workerData.workerId)
	ok := call("Coordinator.GetTask", argsPtr, replyPtr)
	if ok {
		err, workerData.task = unmarshal()
		if err != nil {
			log.Fatalf("GetTask (Worker): unmarshal failed")
		}

		// If new task is available
		if workerData.task != nil {
			workerData.taskType = workerData.task.TaskType()
			workerData.progress = 0     // Reset progress
			workerData.complete = false // Reset complete
		} else {
			workerData.taskType = NO_TASK_TYPE
			fmt.Printf("GetTask (WorkerId %v): No tasks available.\n", workerData.workerId)
		}

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("GetTask failed")
		return err
	}
}

func Execute() (err error) {
	//fmt.Printf("Execute: WorkerId %v.\n", workerData.workerId)
	// If map task, Worker calls map
	// If reduce task, Worker calls reduce

	switch workerData.taskType {
	case MAP_TASK_TYPE:
		filename := workerData.task.(MapTask).inputSlice.filename
		content := Read(filename)

		intermediate := workerData.mapf(filename, string(content))

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

			oname := fmt.Sprintf(tmpPrefix+"%v-%v", workerData.task.Id(), ihash(key))
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
			workerData.progress = int(math.Round(float64((i * 100) / len(intermediate))))

			// *** TEMP FAKE DELAY ***
			time.Sleep(1 * time.Millisecond)
		}
		workerData.complete = true // Officially mark as complete (as opposed to regular reporting)
		fmt.Printf("Execute (Worker): WorkerId: %v, TaskId: %v, Complete: %v\n", workerData.workerId, workerData.task.Id(), workerData.complete)

	case REDUCE_TASK_TYPE:
		//content := Read(task.(MapTask).inputSlice.filename)
		//workerData.reducef(task.(MapTask).inputSlice.filename, content)
	}

	return nil
}

func ReportStatus() {
	//fmt.Printf("ReportStatus: WorkerId: %v, TaskId: %v, Progress: %v, Complete: %t\n",
	//	workerData.workerId, workerData.task.Id(), workerData.progress, workerData.complete)

	argsPtr, replyPtr, _ := MarshalTaskStatusCall(workerData.workerId, workerData.task, workerData.progress, workerData.complete)

	ok := call("Coordinator.TaskStatus", argsPtr, replyPtr)
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
