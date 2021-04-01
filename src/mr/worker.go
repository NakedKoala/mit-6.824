package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"
import "sort"
import "path/filepath"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()


	// fmt.Printf("Map phase begin \n")

	for {
		reply := RequestMapTask(1)

		if reply.NoMoreMapTask {
			break
		}

		if len(reply.Files) > 0 {
			// fmt.Printf("executing map task %v \n", reply)
			ExecuteMapTask(&reply, mapf)
			NotifyMapTaskComplete(&reply)
		}

		time.Sleep(100 * time.Millisecond)
		
	}

	// fmt.Printf("Map phase completed. Reduce phase begin \n")

	for {
		reply :=  RequestReduceTask()
	

		if reply.ReducePartition != -1 {
			// fmt.Printf("executing reduce task %v \n", reply)
			ExecuteReduceTask(&reply, reducef)
			NotifyReduceTaskComplete(&reply)
		}

		time.Sleep(100 * time.Millisecond)

	}

	

}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}


func NotifyMapTaskComplete(requestReply *MapTaskReplyArgs){


	// fmt.Printf("Map task %v complete. Notify coordinator", requestReply)

	args :=  MapTaskCompleteNotificationRequestArgs{}
	args.Files = requestReply.Files 

	reply := MapTaskCompleteNotificationReplyArgs{}
	call("Coordinator.HandleMapTaskCompleteNotification", &args, &reply)
}


func NotifyReduceTaskComplete(requestReply *ReduceTaskReplyArgs ){

	// fmt.Printf("Reduce task %v complete. Notify coordinator", requestReply)

	args :=  ReduceTaskCompleteNotificationRequestArgs{}
	args.ReducePartition = requestReply.ReducePartition

	reply := ReduceTaskCompleteNotificationReplyArgs{}
	call("Coordinator.HandleReduceTaskCompleteNotification", &args, &reply)
}


func FilterDirsGlob(dir, suffix string) ([]string, error) {
    return filepath.Glob(filepath.Join(dir, suffix))
}


func ExecuteReduceTask(reply *ReduceTaskReplyArgs, reducef func(string, []string) string){

	partitionGlobPattern := fmt.Sprintf("M-*-%v.json", reply.ReducePartition)

	intermediate := []KeyValue{}
	fnames, err := FilterDirsGlob("./", partitionGlobPattern)
	if err != nil {
		// TODO: Handle error.
		log.Fatal("can not locate intermediate json file for reduce partiton %v error code %v", reply.ReducePartition  ,err)
	}
	for _, fname := range fnames {
		file, err := os.Open(fname)

		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		dec := json.NewDecoder(file)

		for {
		  var kv KeyValue
		  if err := dec.Decode(&kv); err != nil {
			break
		  }
		  intermediate = append(intermediate, kv)
		}
	}


	sort.Sort(ByKey(intermediate))


	oname := fmt.Sprintf("mr-out-%v", reply.ReducePartition)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
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



func ExecuteMapTask(reply *MapTaskReplyArgs, mapf func(string, string) []KeyValue){
	
	intermediate := []KeyValue{}
	
	for _, filename := range reply.Files {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate= append(intermediate, kva...)
	}


	enc_lst := []*json.Encoder{}

	for i := 0; i < reply.NReduce; i++ {

		outFileName := fmt.Sprintf("./M-%v-%v.json",  reply.TaskID, i)
		file, err := os.Create(outFileName)
		if err != nil {
			log.Fatalf("cannot open %v", outFileName)
		}
		enc := json.NewEncoder(file)
		enc_lst = append(enc_lst, enc)
	} 

	for _, kv := range intermediate{

		reduce_i := ihash(kv.Key) % reply.NReduce
		enc_lst[reduce_i].Encode(&kv)		
	}
	
}

func RequestReduceTask() ReduceTaskReplyArgs {

	args := ReduceTaskRequestArgs{}

	reply := ReduceTaskReplyArgs{}
	
	call("Coordinator.HandleReduceTaskRequest", &args, &reply)
	
	return reply
	
}

func RequestMapTask(num_files int) MapTaskReplyArgs {

	args := MapTaskRequestArgs{}
	args.NumFiles = num_files

	reply := MapTaskReplyArgs{}
	call("Coordinator.HandleMapTaskRequest", &args, &reply)

	return reply

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
		// log.Fatal("dialing:", err)
		// fmt.Print("Coordinator exited. Job is complete. Worker exiting")
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
