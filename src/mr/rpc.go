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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type EmptyArg struct{
}

type StringReply struct {
	content string
}

type MapTask struct{
	Taskid int
	R int//the num of reduce task
	File string//the file map task need to calculate
}

type ReduceTask struct{
	Taskid int
	MaxMapTaskId int//the max value of task id of map
}

type TaskLocator struct {
	Taskid int 
	TaskType string//the type of task,"map" or "reduce"
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
