package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	go SendSignal()
	for {
		// Your worker implementation here.
		taskType, taskId := RequestTask()

		if taskType == "map" {
			intermediate := []KeyValue{}
			file, err := os.Open(fmt.Sprintf("../main/%s", taskId))
			if err != nil {
				log.Fatalf("cannot open %v", taskId)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", taskId)
			}
			file.Close()
			kva := mapf(taskId, string(content))
			intermediate = append(intermediate, kva...)
			writeToIntermediateFile(intermediate, taskId)
			sendProgressUpdate("completed", "map", taskId)
		} else if taskType == "reduce" {
			intermediateFiles,_ := filepath.Glob(fmt.Sprintf("../main/mr-*.txt-%s", taskId))
			intermediate := []KeyValue{}
			oname := fmt.Sprintf("../main/mr-out-%s", taskId)
			ofile, _ := os.Create(oname)
			for _, intermediateFile := range intermediateFiles {
				file, err := os.Open(intermediateFile)
				if err != nil {
					log.Fatalf("cannot open %v", intermediateFile)
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					tokens := strings.Split(line, " ")
					intermediate = append(intermediate, KeyValue{Key: tokens[0], Value: tokens[1]})
				}
			}
			i := 0
			sort.Sort(ByKey(intermediate))
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		
				i = j
			}
		
			ofile.Close()
		}
	}
}

func checkFileExists(filePath string) bool {
	_, error := os.Stat(filePath)
	//return !os.IsNotExist(err)
	return !errors.Is(error, os.ErrNotExist)
}

func writeToIntermediateFile(intermediate []KeyValue, mapTask string) {
	for i := 0; i < len(intermediate); i++ {
		reduceTaskNumber := ihash(intermediate[i].Key) % nReduceTasks;
		intermediateFileName := fmt.Sprintf("mr-%s-%v", mapTask, reduceTaskNumber);
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err.Error())
			return
		}
		fmt.Fprintf(intermediateFile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
		intermediateFile.Close()
	}
}

func RequestTask() (string, string) {

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
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return reply.TaskType, reply.TaskId
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

func sendProgressUpdate(progress string, taskType string, taskId string) {
	// declare an argument structure.
	args := RPCArgs{}

	// fill in the argument(s).
	args.RequestType = "update"
	args.WorkerId = os.Getpid()
	args.Message = progress
	args.Task = taskType
	args.TaskId = taskId

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
