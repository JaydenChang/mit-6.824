package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArgs struct {
	// X int
	Msg string
}

<<<<<<< HEAD
type TaskReply struct {
	XTask         Task
	FileName      string
	NumMapTask    int
	NumReduceTask int
	CurMapNum     int
	CurReduceNum  int
}

// Add your RPC definitions here.
=======
type TaskRequest struct {
	X int
}

type TaskReply struct {
	XTask            Task
	NumMapTask       int
	NumReduceTask    int
	State            int32
	CurNumMapTask    int
	CurNumReduceTask int
}
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
