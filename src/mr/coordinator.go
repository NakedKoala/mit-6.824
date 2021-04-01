package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "os/exec"


type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int

	MapNotStarted map[string]bool
	MapInProgress map[string]bool
	MapDone map[string]bool

	ReduceNotStarted map[int]bool
	ReduceInProgress map[int]bool
	ReduceDone map[int]bool

	mapTaskCounter int
	reduceTaskCounter int 
	mu sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ResetInProgressReduceTask(reply *ReduceTaskReplyArgs) error {

	time.Sleep(10 * time.Second)

	c.mu.Lock()

	if _, ok := c.ReduceDone[reply.ReducePartition]; !ok {
		// fmt.Printf("Reduce task %v not done after 10 second, remove from ReduceInProgress return to ReduceNotStarted", reply)
		delete(c.ReduceInProgress, reply.ReducePartition)
		c.ReduceNotStarted[reply.ReducePartition] = true 
	}

	c.mu.Unlock()

	return nil
}

func (c *Coordinator) ResetInProgressMapTask(reply *MapTaskReplyArgs) error {

	time.Sleep(10 * time.Second)
	c.mu.Lock()
	for _, fname := range reply.Files {
		if _, ok := c.MapDone[fname]; !ok {
			// fmt.Printf("Map task %v not done after 10 second, remove from MapInProgress return to MapNotStarted", fname)
			delete(c.MapInProgress, fname)
			c.MapNotStarted[fname] = true 
		}
	}
	c.mu.Unlock()
	return nil

}



func (c *Coordinator) HandleReduceTaskCompleteNotification(args *ReduceTaskCompleteNotificationRequestArgs, reply *ReduceTaskCompleteNotificationReplyArgs) error {

	c.mu.Lock()

	delete(c.ReduceInProgress, args.ReducePartition)
	c.ReduceDone[args.ReducePartition] = true 
	
	c.mu.Unlock()

	return nil

}

func (c *Coordinator) HandleMapTaskCompleteNotification(args *MapTaskCompleteNotificationRequestArgs, reply *MapTaskCompleteNotificationReplyArgs) error {

	c.mu.Lock()

	for _, fname := range args.Files {
		delete(c.MapInProgress, fname)
		c.MapDone[fname] = true 
	}

	if len(c.ReduceDone) == c.nReduce {

	}

	c.mu.Unlock()

	return nil

}

func (c *Coordinator) HandleReduceTaskRequest(args *ReduceTaskRequestArgs, reply *ReduceTaskReplyArgs) error {

	c.mu.Lock()

	if len(c.MapDone) < len(c.files) ||  len(c.ReduceNotStarted) == 0 {
		// no reduce task to available at the moment
		reply.ReducePartition = -1 
		c.mu.Unlock()
		return nil 
	}

	for k, _ := range c.ReduceNotStarted {
		reply.ReducePartition = k 
		delete(c.ReduceNotStarted, k)
		c.ReduceInProgress[k] = true 
		break
	}


	c.mu.Unlock()

	go c.ResetInProgressReduceTask(reply)

	return nil

}


func (c *Coordinator) HandleMapTaskRequest(args *MapTaskRequestArgs, reply *MapTaskReplyArgs) error {
	
	c.mu.Lock()

	reply.TaskID = c.mapTaskCounter
	c.mapTaskCounter = c.mapTaskCounter + 1
	reply.Files = []string{}
	reply.NReduce = c.nReduce

	for k, _ := range c.MapNotStarted {
		
		if len(reply.Files) == args.NumFiles {
			break
		}
		reply.Files = append(reply.Files, k)
		
		c.MapInProgress[k] = true
		delete(c.MapNotStarted, k)
    }

	reply.NoMoreMapTask = false
	if len(c.MapDone) == len(c.files){
		reply.NoMoreMapTask = true
	}

	c.mu.Unlock()

	go c.ResetInProgressMapTask(reply)

	return nil 
}

func (c *Coordinator) initState(files []string, nReduce int){
	c.files = files 
	c.nReduce = nReduce

	c.MapNotStarted = make(map[string]bool)
	for _, v := range c.files {
		c.MapNotStarted[v] = true 
	}
	c.MapInProgress = make(map[string]bool)
	c.MapDone = make(map[string]bool)
	c.mapTaskCounter = 0




	c.ReduceNotStarted = make(map[int]bool)
	for i := 0; i < c.nReduce; i++ {
		c.ReduceNotStarted[i] = true 
	}
	c.ReduceInProgress = make(map[int]bool)
	c.ReduceDone = make(map[int]bool)
	c.reduceTaskCounter = 0
	// fmt.Printf("coordinator %v\n", c)

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

func RemoveIntermediateFiles() {

    cmd := exec.Command("/bin/bash", "-c", "rm -f M-*")
    _, err := cmd.Output()

    if err != nil {
        fmt.Println(err.Error())
        return
    }
}
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	ret := len(c.ReduceDone) ==  c.nReduce
	c.mu.Unlock()

	if ret {
		RemoveIntermediateFiles()
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {


	c := Coordinator{}
	
	c.initState(files, nReduce)
	
	// Your code here.
	c.server()
	return &c
}
