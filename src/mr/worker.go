package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
	"io/ioutil"
	"math"
)

var nReduceTasks = 10

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
	task := RequestTask()
	go SendSignal()

	intermediate := []KeyValue{}
	file, err := os.Open(fmt.Sprintf("../main/%s", task))
	if err != nil {
		log.Fatalf("cannot open %v", task)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task)
	}
	file.Close()
	kva := mapf(task, string(content))
	intermediate = append(intermediate, kva...)
	writeToIntermediateFile(intermediate, task)
	fmt.Println(task)
}

func writeToIntermediateFile(intermediate []KeyValue, mapTask string) {
	numberOfFiles := len(intermediate)/nReduceTasks
	currentIndex := 0
	for fileNo := 0; fileNo < numberOfFiles; fileNo++ {
		oname := fmt.Sprintf("mr-%s-%v", mapTask, fileNo)
		ofile, _ := os.Create(oname)
		prevIndex := currentIndex
		for currentIndex < int(math.Min(float64(prevIndex + len(intermediate)/numberOfFiles), float64(len(intermediate)))) {
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[currentIndex].Key, intermediate[currentIndex].Value)
			currentIndex += 1
		}
	}
}

func RequestTask() string {

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
		fmt.Printf("reply.Y %v\n", reply.MapTask)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.MapTask
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
			fmt.Println("Ping coordinator successfully")
		} else {
			fmt.Printf("call failed!\n")
		}
		time.Sleep(1 * time.Second)
	}
}

func sendProgressUpdate(progress string) {
	// declare an argument structure.
	args := RPCArgs{}

	// fill in the argument(s).
	args.RequestType = "update"
	args.WorkerId = os.Getpid()
	args.Message = progress

	// declare a reply structure.
	reply := RPCReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		fmt.Println("Sent progress update successfully")
	} else {
		fmt.Printf("call failed!\n")
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
