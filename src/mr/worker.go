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
)

var workerId int
var task Task
var progress int

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapf = mapf
	//reducef = reducef

	// Worker is assigned an ID
	err := Register()
	if err != nil {
		return
	}

	// Start Worker Loop
	// Worker gets task from Coordinator
	err = GetTask()
	if err != nil {
		return
	}

	if task != nil {
		// Worker begins task
		Execute(mapf, reducef)

		// Worker starts goroutine to periodically report task status to Coordinator
		// go Heartbeat()

		// Worker reports task completion to Coordinator
		//Report()

		// uncomment to send the Example RPC to the coordinator.
		//CallExample()
	}
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

	// declare an argument structure.
	args := new(interface{})

	// declare a reply structure.
	reply := RegisterReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Register" tells the
	// receiving server that we'd like to call
	// the Register() method of struct Coordinator.
	ok := call("Coordinator.Register", args, &reply)
	if ok {
		//fmt.Printf("reply.workerId %v\n", reply.WorkerId)
		workerId = reply.WorkerId

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("Register failed")
		return err
	}
}

func GetTask() (err error) {

	// declare an argument structure.
	args := TaskArgs{WorkerId: workerId}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetTask" tells the
	// receiving server that we'd like to call
	// the GetTask() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		//fmt.Printf("reply.WorkerId %v\n", reply.WorkerId)
		//fmt.Printf("reply.TaskType %v\n", reply.TaskType)
		//fmt.Printf("reply.Filename %v\n", reply.Filename)
		//fmt.Printf("reply.ReportInterval %v\n", reply.ReportInterval)
		switch reply.TaskType {
		case MAP_TASK_TYPE:
			task = MapTask{
				id: reply.MapTaskId,
				inputSlice: InputSlice{
					filename: reply.Filename,
				},
			}
		case REDUCE_TASK_TYPE:
		case NO_TASK_TYPE:
			task = nil
		default:
			err := fmt.Errorf("invalid task type %v", reply.TaskType)
			return err
		}

		// Reset progress
		progress = 0

		return nil
	} else {
		fmt.Printf("call failed!\n")

		err := fmt.Errorf("GetTask failed")
		return err
	}
}

func Execute(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (err error) {
	// If map task, Worker calls map
	// If reduce task, Worker calls reduce

	switch task.TaskType() {
	case MAP_TASK_TYPE:
		filename := task.(MapTask).inputSlice.filename
		content := Read(filename)

		intermediate := mapf(filename, string(content))

		//intermediate := []KeyValue{}
		//intermediate = append(intermediate, kva...)

		sort.Sort(ByKey(intermediate))

		// fmt.Printf("task id: %d\n", task.(MapTask).id)

		// Make temporary intermediate data directory
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			os.Mkdir(tmpDir, os.ModeDir|0755)
		}

		// Loop through all key/value pairs
		i := 0
		for i < len(intermediate) {
			key := intermediate[i].Key

			// fmt.Printf("key: %v\n", key)
			// fmt.Printf("ihash: %v\n", ihash(key))

			oname := fmt.Sprintf(tmpPrefix+"%v-%v", task.(MapTask).id, ihash(key))
			ofile, _ := os.Create(tmpDir + "/" + oname)
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
			progress = int(math.Round(float64((i * 100) / len(intermediate))))
			//fmt.Printf("WorkerId: %v, TaskId: %v, Progress: %v\n", workerId, task.(MapTask).id, progress)
		}

		// Report complete
		progress = 100

		// declare an argument structure.
		args := TaskStatusArgs{
			WorkerId:  workerId,
			TaskType:  task.(MapTask).TaskType(),
			MapTaskId: task.(MapTask).id,
			Progress:  progress,
			Complete:  true,
		}

		// declare a reply structure.
		reply := TaskStatusReply{}

		ok := call("Coordinator.TaskStatus", &args, &reply)
		if ok {
		} else {
			fmt.Printf("call failed!\n")
		}

	case REDUCE_TASK_TYPE:
		//content := Read(task.(MapTask).inputSlice.filename)
		//mapf(task.(MapTask).inputSlice.filename, content)
	}

	return nil
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
