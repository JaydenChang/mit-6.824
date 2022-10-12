package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	FileName string
	IDMap    int
	IDReduce int
	TaskType int // 0 map 1 reduce 2 none
}

type Coordinator struct {
	// Your definitions here.
	State            int // 0 map 1 reduce 2 finish
	NumMapTask       int
	NumReduceTask    int
	MapTask          chan Task
	ReduceTask       chan Task
	MapTaskFinish    chan bool
	ReduceTaskFinish chan bool
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

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
	if len(c.MapTaskFinish) != c.NumMapTask {
		mapTask, ok := <-c.MapTask
		if ok {
			reply.XTask = mapTask
		}
		c.State = 0
	} else if len(c.ReduceTaskFinish) != c.NumReduceTask {
		reduceTask, ok := <-c.ReduceTask
		c.State = 1
		if ok {
			reply.XTask = reduceTask
		}
	}
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

func (c *Coordinator) FinishTask(args *TaskRequest, reply *TaskReply) error {
	if len(c.MapTaskFinish) != c.NumMapTask {
		c.MapTaskFinish <- true
		if len(c.MapTaskFinish) == c.NumMapTask {
			c.State = 1
		}
	} else if len(c.ReduceTaskFinish) != c.NumReduceTask {
		c.ReduceTaskFinish <- true
		if len(c.ReduceTaskFinish) == c.NumReduceTask {
			c.State = 2
		}
	}
	return nil
}

func (c *Coordinator) EndTask(args *TaskRequest, reply *TaskReply) (int, error) {
	if len(c.ReduceTaskFinish) == c.NumReduceTask {
		return 1, nil
	} else {
		return 0, nil
	}
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

	// time.Sleep(time.Second)
	if len(c.ReduceTaskFinish) == c.NumReduceTask {
		c.State = 2
		time.Sleep(time.Second)
		ret = true
	}

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:            0,
		NumMapTask:       len(files),
		NumReduceTask:    nReduce,
		MapTask:          make(chan Task, len(files)),
		ReduceTask:       make(chan Task, nReduce),
		MapTaskFinish:    make(chan bool, len(files)),
		ReduceTaskFinish: make(chan bool, nReduce),
	}

	for id, file := range files {
		c.MapTask <- Task{
			FileName: file,
			IDMap:    id,
			TaskType: 0,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{
			IDReduce: i,
			TaskType: 1,
			FileName: "",
		}
	}

	// Your code here.

	c.server()
	return &c
}
