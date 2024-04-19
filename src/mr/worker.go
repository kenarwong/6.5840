package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

var workerId int
var task Task

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
	Register()

	// Worker gets task from Coordinator
	GetTask()

	// Worker begins task
	Execute(mapf, reducef)

	// Worker starts goroutine to periodically report task status to Coordinator
	// go Heartbeat()

	// Worker reports task completion to Coordinator
	//Report()

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

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

func Register() {

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
		fmt.Printf("reply.workerId %v\n", reply.WorkerId)
		workerId = reply.WorkerId
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetTask() {

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
		fmt.Printf("reply.WorkerId %v\n", reply.WorkerId)
		fmt.Printf("reply.TaskType %v\n", reply.TaskType)
		fmt.Printf("reply.Filename %v\n", reply.Filename)
		fmt.Printf("reply.ReportInterval %v\n", reply.ReportInterval)
		switch reply.TaskType {
		case MAP_TASK_TYPE:
			task = MapTask{
				id: reply.MapTaskId,
				inputSlice: InputSlice{
					filename: reply.Filename,
				},
			}
		case REDUCE_TASK_TYPE:
		}
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Execute(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// If map task, Worker calls map
	// If reduce task, Worker calls reduce

	switch task.TaskType() {
	case MAP_TASK_TYPE:
		filename := task.(MapTask).inputSlice.filename
		content := Read(filename)

		kva := mapf(filename, string(content))

		intermediate := []KeyValue{}
		intermediate = append(intermediate, kva...)

		sort.Sort(ByKey(intermediate))

		// fmt.Printf("task id: %d\n", task.(MapTask).id)

		i := 0
		for i < len(intermediate) {
			key := intermediate[i].Key

			// fmt.Printf("key: %v\n", key)
			// fmt.Printf("ihash: %v\n", ihash(key))

			oname := fmt.Sprintf("mr-%v-%v", task.(MapTask).id, ihash(key))
			ofile, _ := os.Create(oname)
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
		}

	case REDUCE_TASK_TYPE:
		//content := Read(task.(MapTask).inputSlice.filename)
		//mapf(task.(MapTask).inputSlice.filename, content)
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
