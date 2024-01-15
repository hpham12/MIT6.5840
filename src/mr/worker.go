package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	RequestTask()

	go SendSignal()
}

func RequestTask() {

	// declare an argument structure.
	args := RPCArgs{}

	// fill in the argument(s).
	args.RequestType = "task"
	args.WorkerId = os.Getpid()

	// declare a reply structure.
	reply := RPCReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.MapTask)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func SendSignal() {

	// declare an argument structure.
	args := RPCArgs{}

	// fill in the argument(s).
	args.RequestType = "ping"
	args.WorkerId = os.Getpid()

	// declare a reply structure.
	reply := RPCReply{}

	for {
		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.RPCHandler", &args, &reply)
		if ok {
			// reply.Y should be 100.
			fmt.Printf("reply.MapTask %v\n", reply.MapTask)
		} else {
			fmt.Printf("call failed!\n")
		}
		time.Sleep(1 * time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
