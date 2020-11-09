package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"


type Master struct {
	// Your definitions here.
	input_files []string //list of input files
	input_status []string //status of input_files
	status string //the status of the program ,map or reduce
	reduceTaskNum int//the amount of reduce task
	reduce_status []string // the status of reduce task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//get the status of program
func (m *Master) GetStatus(args EmptyArg,status *string) error{
	*status=m.status
	return nil
}

//check the task status after 10 seconds.if it is not be done , the task will be redo
func waitForDone(status *string){
	time.Sleep(time.Second*10)
	if *status != "done" {
		*status = "wait"
	}
}

//get a MapTask
func (m *Master) GetMapTask(args *EmptyArg,reply *MapTask) error{
	for i,filename := range m.input_files{
		if m.input_status[i]=="wait"{
			reply.Taskid=i 
			reply.File=filename
			reply.R=m.reduceTaskNum
			m.input_status[i]="processing"
			go waitForDone(&(m.input_status[i]))
			fmt.Println("reply:taskid=",reply.Taskid," file=",reply.File)
			return nil
		}
	}
	reply.File=""
	if m.status=="reduce" {
		//worker shoud do the reduce 
		reply.Taskid=-2
	}else{
		reply.Taskid=-1
	}
	return nil
}

//update the status of master from "map" to "reduce" or from "reduce" to stop
func (m *Master) UpdateStatus(status_list []string,target_status string){
	var i int
	for i=0;i<len(status_list);i++ {
		if status_list[i]!="done"{
			return
		}
	}
	//now,all the task have been done 
	m.status=target_status
	return
}



//notify the master that a task is done 
func (m *Master) TaskDone(task_locator TaskLocator,result *string) error{
	var status []string 
	var target_status string
	if task_locator.TaskType=="map"{
		status=m.input_status
		target_status="reduce"
	}else{
		status=m.reduce_status
		target_status="stop"
	}

	status[task_locator.Taskid]="done"
	*result=status[task_locator.Taskid]
	fmt.Printf("%s task %d done\n",task_locator.TaskType,task_locator.Taskid)
	go m.UpdateStatus(status,target_status)
	return nil
}

//get a reduce's input file name.If there is no file need to be reduced,it will return empty
//if there is no file need to be reduce,reduce taskid as -1
//if the program is stoped ,reduce taskid as -2 
func (m *Master) GetReduceTask(args *EmptyArg,reply *ReduceTask) error{
	reply.MaxMapTaskId=len(m.input_files)
	if m.status != "reduce" {
		//the app is to stop 
		reply.Taskid=-2
		return nil
	}
	for i,status := range m.reduce_status{
		if status == "wait" {
			reply.Taskid=i 
			m.reduce_status[i]="processing"
			go waitForDone(&(m.reduce_status[i]))
			return nil
		}
	}
	reply.Taskid=-1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret=m.status=="stop"

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	//initialize map status
	m.input_files=files 
	m.input_status=make([]string,len(m.input_files))
	m.reduceTaskNum=nReduce
	for i,_ := range m.input_files {
		m.input_status[i]="wait"
	}

	m.reduce_status=make([]string,m.reduceTaskNum)
	for i:=0;i<m.reduceTaskNum;i++{
		m.reduce_status[i]="wait"
	}

	fmt.Println("input_files:",m.input_files)
	fmt.Println("input_status:",m.input_status)

	//make a thread periodly print the status of master
	go m.PrintStatus()
	//go m.Cleaner()

	m.server()
	return &m
}

func (m *Master)PrintStatus(){
	for true{
		time.Sleep(time.Second)
		fmt.Printf("status: %s\n",m.status)
	}
}

//check for status . if status == clean ,do the clean work
func (m *Master)Cleaner(){
	if m.status!="clean"{
		time.Sleep(time.Second)
	}else{
		for i:=0;i<len(m.input_files);i++{
			for j:=0;j<m.reduceTaskNum;j++{
				path:=fmt.Sprintf("mr-%d-%d.json",i,j)
				os.Remove(path)
			}
		}
		m.status="stop"
	}
}