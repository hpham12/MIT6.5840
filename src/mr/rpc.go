package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.
type RPCArgs struct {
	RequestType string
	WorkerId int // for this case, use process id as the workerId
	Message string
}

type RPCReply struct {
	MapTask string // name of the file to apply map function
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
