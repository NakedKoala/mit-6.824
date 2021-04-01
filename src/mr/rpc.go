package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ReduceTaskCompleteNotificationRequestArgs struct {
	ReducePartition int
}

type ReduceTaskCompleteNotificationReplyArgs struct {
	
}

type MapTaskCompleteNotificationRequestArgs struct {
	Files []string
}

type MapTaskCompleteNotificationReplyArgs struct {
	
}


type ReduceTaskRequestArgs struct {
	
}

type ReduceTaskReplyArgs struct {
	ReducePartition int
}


type MapTaskRequestArgs struct {
	NumFiles int 
}

type MapTaskReplyArgs struct {
	NReduce int
	TaskID int
	Files []string
	NoMoreMapTask bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
