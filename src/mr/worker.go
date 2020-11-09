package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "io/ioutil"
import "os"
import "encoding/json"

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

	// uncomment to send the Example RPC to the master.
	//CallExample()
	var ok bool

	emptyArg := EmptyArg{}
	mapTask := MapTask{}
	task_locator:=TaskLocator{}
	task_locator.TaskType="map"
	result:=""
	//get the filename need to be mapped
	for true{
		ok=call("Master.GetMapTask",&emptyArg,&mapTask) 
		if !ok{
			os.Exit(0)
		}
		if mapTask.Taskid==-1{
			time.Sleep(time.Second)
			continue 
		}else if mapTask.Taskid==-2{
			//status go to "reduce"
			break
		}

		//do the map task here
		bytes,err := ioutil.ReadFile(mapTask.File)
		if err!=nil{
			fmt.Println("worker going to do map failed.file:",mapTask.File,err)
		}
		kvs := mapf(mapTask.File,string(bytes))

		//write the Key-Value pairs in the map
		var r2kv map[int][]KeyValue 
		r2kv=make(map[int][]KeyValue)
		for i:=0;i<mapTask.R;i++{
			r2kv[i]=make([]KeyValue,0)
		}
		for _,kv := range kvs{
			hashvalue:=ihash(kv.Key)%mapTask.R
			kvaInMap,ok := r2kv[hashvalue]
			if !ok{
				r2kv[hashvalue]=make([]KeyValue,1)
				r2kv[hashvalue][0]=kv
			}else{
				r2kv[hashvalue]=append(kvaInMap,kv)
			}
		}

		//write it to the files
		for i:=0;i<mapTask.R;i++ {
			kvToWrite,ok:=r2kv[i]
			if ok {
				outputPath:=fmt.Sprintf("mr-%v-%v.json",mapTask.Taskid,i)
				fileObj,err:=os.Create(outputPath)
				if err!= nil{
					fmt.Println("fatal:open the output file error")
					fmt.Println("err:",err,"\npath:",outputPath)
				}
				b,err:=json.Marshal(kvToWrite)
				_,err=fileObj.Write(b)
				if err!=nil{
					fmt.Println("fatal:write json to file fail")
					fmt.Println("err:",err)
				}
			}
		}

		//notify to the master that the map task is done
		task_locator.Taskid=mapTask.Taskid
		ok=call("Master.TaskDone",&task_locator,&result)
		if !ok {
			os.Exit(0)
		}
	}
	fmt.Println("\n\n\ngoing to reduce\n\n\n")

	//do the reduce
	var empty EmptyArg 
	var reduce_task ReduceTask
	task_locator.TaskType="reduce"
	for true{
		call("Master.GetReduceTask",&empty,&reduce_task)
		if reduce_task.Taskid==-1{
			time.Sleep(time.Second)
			continue
		}else if reduce_task.Taskid==-2{
			break
		}
		fmt.Printf("run reduce\n")
		err:=RunReduce(reduce_task,reducef)
		fmt.Printf("reduce done,err=%v\n",err)
		task_locator.Taskid=reduce_task.Taskid
		call("Master.TaskDone",&task_locator,&result)
	}
}

//get the input file and run the reduce task and output
func RunReduce(reduce_task ReduceTask,reducef func(string, []string) string) error{
	var i int 
	var key2values=make(map[string][]string)
	var values []string
	var key_values []KeyValue
	var err error
	var ok bool 

	//collect the values by key
	var json_bytes []byte
	var path string
	var kv KeyValue
	for i=0;i<reduce_task.MaxMapTaskId;i++ {
		path=fmt.Sprintf("mr-%d-%d.json",i,reduce_task.Taskid)
		json_bytes,err=ioutil.ReadFile(path)
		if err!= nil {
			return err 
		}
		err=json.Unmarshal(json_bytes,&key_values)
		if err!=nil{
			return err 
		}
		for _,kv = range key_values{
			values,ok=key2values[kv.Key]
			if !ok{
				values=make([]string,0)
			}
			values=append(values,kv.Value)
			key2values[kv.Key]=values
		}
	}

	//run the reduce func and get the result
	var key,reduce_result string
	var output_string=""
	var line=""
	for key,values = range key2values{
		reduce_result=reducef(key,values)
		line=fmt.Sprintf("%v %v\n",key,reduce_result)
		output_string+=line
	} 

	//output
	path=fmt.Sprintf("mr-out-%d",reduce_task.Taskid)
	file,err:=os.Create(path)
	if err!=nil{
		return err
	}
	_,err=file.WriteString(output_string)
	return err
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	fmt.Println("rpc:",rpcname,args,reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
