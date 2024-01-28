package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"strconv"
)


type WorkerInfo struct {
	status bool // false for down, true for up
	lastPing int64 // time of last ping
}

type IntermediateFileInfo struct {
	size int
	location string
}

type MapTask struct {
	mapTaskId string
	currentWorker int // pid of the current worker
	intermediateFiles []IntermediateFileInfo
}

type ReduceTask struct {
	reduceTaskId string
	currentWorker int // pid of the current worker
}

type Coordinator struct {
	workersInfoMapping map[int]*WorkerInfo // map worker PID to worker information
	mapTasks map[string]*list.List // keys are: idle, completed, and in-progress
	reduceTasks map[string]*list.List // keys are: idle, completed, and in-progress
	mapTaskLock sync.Mutex
	reduceTasksLock sync.Mutex
	workersLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *RPCArgs, reply *RPCReply) error {
	requestType := args.RequestType
	workerId := args.WorkerId
	if requestType == "task" {
		c.workersLock.Lock()
		// start scheduling reduce task when all map tasks are finished
		if c.mapTasks["idle"].Len() == 0 && c.mapTasks["in-progress"].Len() == 0 {
			// distribute reduce tasks
			currentElement := c.reduceTasks["idle"].Front()
			if currentElement != nil {
				currentReduceTask := currentElement.Value.(ReduceTask)
				currentReduceTask.currentWorker = workerId
				currentReduceTaskId := currentReduceTask.reduceTaskId
				reply.TaskId = currentReduceTaskId
				reply.TaskType = "reduce"
				if c.workersInfoMapping[workerId] != nil {
					c.workersInfoMapping[workerId] = &WorkerInfo{true, time.Now().Unix()}
				}
				// Remove task from idle list
				(*c.reduceTasks["idle"]).Remove(currentElement)
		
				// Move task to in-progress list
				(*c.reduceTasks["in-progress"]).PushBack(currentReduceTask)
			}
		} else {
			currentElement := c.mapTasks["idle"].Front()
			if currentElement != nil {
				currentMapTask := currentElement.Value.(MapTask)
				currentMapTask.currentWorker = workerId
				currentMapTaskId := currentMapTask.mapTaskId
				reply.TaskId = currentMapTaskId
				reply.TaskType = "map"
				c.workersInfoMapping[workerId] = &WorkerInfo{true, time.Now().Unix()}
				
				// Remove task from idle list
				(*c.mapTasks["idle"]).Remove(currentElement)
	
				// Move task to in-progress list
				(*c.mapTasks["in-progress"]).PushBack(currentMapTask)
			}
		}
	} else if requestType == "ping" {
		c.workersLock.Lock()
		val, ok := c.workersInfoMapping[workerId]
		if ok {
			if time.Now().Unix() - val.lastPing >= 10 {
				c.handleFailedWorker(workerId)
			} else {
				c.workersInfoMapping[workerId].lastPing = time.Now().Unix()
			}
		}
	} else if requestType == "update" {
		c.workersLock.Lock()
		if (args.Task == "map") {
			mapTaskId := args.TaskId
			var completedMapTaskToBeRemoved *list.Element
			// Remove task from idle list
			for mapTask := c.mapTasks["in-progress"].Front(); mapTask != nil; mapTask = mapTask.Next() {
				if mapTask.Value.(MapTask).mapTaskId == mapTaskId {
					(*c.mapTasks["completed"]).PushBack(mapTask.Value.(MapTask))
					completedMapTaskToBeRemoved = mapTask
				}
			}
			if completedMapTaskToBeRemoved != nil {
				c.mapTasks["in-progress"].Remove(completedMapTaskToBeRemoved)
			}
			if c.mapTasks["idle"].Len() == 0 && c.mapTasks["in-progress"].Len() == 0 {
				for i := 0 ; i < 10; i++ {
					c.reduceTasks["idle"].PushBack(ReduceTask{reduceTaskId: strconv.Itoa(i)})
				}
			}
		} else if args.Task == "reduce" {

		}
	}
	c.workersLock.Unlock()
	return nil
}

func (c *Coordinator) handleFailedWorker(workerId int) {
	completedMapTasks := c.mapTasks["completed"]
	inProgressMapTasks := c.mapTasks["in-progress"]
	completedTasksToBeRemoved := []*list.Element{}
	inProgressTasksToBeRemoved := []*list.Element{}
	c.mapTaskLock.Lock()
	for mapTask := completedMapTasks.Front(); mapTask != nil; mapTask = mapTask.Next() {
		if mapTask.Value.(MapTask).currentWorker == workerId {
			c.mapTaskLock.Lock()
			(*c.mapTasks["idle"]).PushBack(mapTask.Value.(MapTask))
			completedTasksToBeRemoved = append(completedTasksToBeRemoved, mapTask)
		}
	}
	for mapTask := inProgressMapTasks.Front(); mapTask != nil; mapTask = mapTask.Next() {
		if mapTask.Value.(MapTask).currentWorker == workerId {
			(*c.mapTasks["idle"]).PushBack(mapTask.Value.(MapTask))
			inProgressTasksToBeRemoved = append(inProgressTasksToBeRemoved, mapTask)
		}
	}

	for _,completedTaskToBeRemoved := range completedTasksToBeRemoved{
		completedMapTasks.Remove(completedTaskToBeRemoved)
	}

	for _,inProgressTaskToBeRemoved := range inProgressTasksToBeRemoved{
		inProgressMapTasks.Remove(inProgressTaskToBeRemoved)
	}
	c.mapTaskLock.Unlock()
	delete(c.workersInfoMapping, workerId)
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
	go c.healthCheck()
}

func (c *Coordinator) healthCheck() {
	for {
		c.workersLock.Lock()
		for k, v := range c.workersInfoMapping {
			if time.Now().Unix() - v.lastPing >= 10 {
				c.handleFailedWorker(k)
			}
		}
		c.workersLock.Unlock()
		time.Sleep(1 * time.Second)
	}
	
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
	nReduceTasks = nReduce;
	c := Coordinator{}
	c.mapTasks = make(map[string]*list.List)
	c.reduceTasks = make(map[string]*list.List)
	// Your code here.
	c.workersInfoMapping = make(map[int]*WorkerInfo)
	c.mapTasks["in-progress"] = list.New()
	c.mapTasks["completed"] = list.New()
	c.reduceTasks["idle"] = list.New()
	c.reduceTasks["in-progress"] = list.New()
	c.reduceTasks["completed"] = list.New()

	mapTasksQueue := list.New()
	for _,file := range files {
		mapTask := MapTask{}
		mapTask.mapTaskId = file
		mapTasksQueue.PushBack(mapTask)
	}
	
	c.mapTasks["idle"] = mapTasksQueue

	c.server()
	return &c
}
