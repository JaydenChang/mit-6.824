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
	TaskType      int // 0 map 1 reduce 2 none 3 exit
	FileName      string
	FailedWorkers []int // record workerID who failed to process the task in 10s
	TaskState     int   // 0 idle 1 running 2 finished
	IDMap         int
	IDReduce      int
}

type Coordinator struct {
	// Your definitions here.
	numWorkers       int
	mapTasks         chan Task
	reduceTasks      chan Task
	nReduce          int
	taskPhase        int // 为了提示快速分配对应类型的任务
	MapTaskFinish    chan bool
	ReduceTaskFinish chan bool
	// mutex       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetMapTask(args *TaskResponse, reply *TaskReply) error {
	mapTask, ok := <-c.mapTasks
	if ok {
		reply.ReplyTask = mapTask
	}
	reply.NumReduceTask = c.nReduce
	reply.MapTaskFinish = c.MapTaskFinish
	reply.ReduceTaskFinish = c.ReduceTaskFinish
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

	if len(c.ReduceTaskFinish) == c.nReduce {
		ret = true
	}

	time.Sleep(time.Second)
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
		numWorkers:       0,
		mapTasks:         make(chan Task, len(files)),
		reduceTasks:      make(chan Task, nReduce),
		MapTaskFinish:    make(chan bool, len(files)),
		ReduceTaskFinish: make(chan bool, nReduce),
		nReduce:          nReduce,
		taskPhase:        0,
		// mutex:       &sync.Mutex{},
	}

	for id, file := range files {
		c.mapTasks <- Task{
			TaskType:      0,
			FileName:      file,
			FailedWorkers: nil,
			TaskState:     0,
			IDMap:         id,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- Task{
			TaskType: 1,
			IDReduce: i,
		}
	}
	// Your code here.

	c.server()
	return &c
}
