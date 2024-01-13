package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerInfo struct {
	reduceTasks []string // list of reduce tasks
	mapTasks []string // list of map tasks
	status bool // 0 for down, 1 for up
}

type IntermediateFileInfo struct {
	size int
	location string
}

type MapTaskInfo struct {
	mapTaskId string
	currentWorker int // pid of the current worker
	intermediateFiles []IntermediateFileInfo
}

type ReduceTaskInfo struct {
	reduceTaskId string
	currentWorker int // pid of the current worker
}

type Coordinator struct {
	workersInfoMapping map[int]WorkerInfo // map worker PID to worker information
	mapTasksInfoMapping map[string][]MapTaskInfo // keys are: idle, completed, and in-progress
	reduceTasksInfoMapping map[string][]ReduceTaskInfo // keys are: idle, completed, and in-progress
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *RPCArgs, reply *RPCReply) error {
	if args.RequestType == "task" {
		reply.MapTask = c.mapTasksInfoMapping["idle"][0].mapTaskId
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.workersInfoMapping = make(map[int]WorkerInfo)

	mapTasksMapping := make(map[string][]MapTaskInfo)
	mapTasksMapping["idle"] = []MapTaskInfo{}
	mapTasksMapping["in-progress"] = []MapTaskInfo{}
	mapTasksMapping["completed"] = []MapTaskInfo{}



	for _,file := range files {
		mapTaskInfo := MapTaskInfo{}
		mapTaskInfo.mapTaskId = file
		mapTasksMapping["idle"] = append(mapTasksMapping["idle"], mapTaskInfo)
	}
	
	c.mapTasksInfoMapping = mapTasksMapping

	c.server()
	return &c
}
