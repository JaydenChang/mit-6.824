package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	FileName string
	IDMap    int
	IDReduce int
}

type Coordinator struct {
	// Your definitions here.
	State          int32 // 0 map 1 reduce 2 finish
	NumMapTask     int
	NumReduceTask  int
	MapTask        chan Task
	ReduceTask     chan Task
	MapTaskTime    sync.Map
	ReduceTaskTime sync.Map
	files          []string
}

type TimeStamp struct {
	Time   int64
	Finish bool
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
	state := atomic.LoadInt32(&c.State)
	if state == 0 {
		if len(c.MapTask) != 0 {
			mapTask, ok := <-c.MapTask
			if ok {
				reply.XTask = mapTask
			}
			reply.CurNumMapTask = len(c.MapTask)
		} else {
			reply.CurNumMapTask = -1
		}
		reply.CurNumReduceTask = len(c.ReduceTask)
	} else if state == 1 {
		if len(c.ReduceTask) != 0 {
			reduceTask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reduceTask
			}
			reply.CurNumReduceTask = len(c.ReduceTask)
		} else {
			reply.CurNumReduceTask = -1
		}
		reply.CurNumMapTask = -1
	}
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = state
	return nil
}

// func lenSyncMap(m *sync.Map) int {
// 	i := 0
// 	m.Range(func(key, value interface{}) bool {
// 		i++
// 		return true
// 	})
// 	return i
// }

func lenFinishTask(m *sync.Map) int {
	i := 0
	m.Range(func(key, value interface{}) bool {

		if value.(TimeStamp).Finish {
			i++
		}
		return true
	})
	return i
}

func (c *Coordinator) FinishTask(args *Task, reply *TaskReply) error {
	time_now := time.Now().Unix()
	if lenFinishTask(&c.MapTaskTime) != c.NumMapTask {
		start_time, _ := c.MapTaskTime.Load(args.IDMap)
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.MapTaskTime.Store(args.IDMap, TimeStamp{time_now, true})
		if lenFinishTask(&c.MapTaskTime) == c.NumMapTask {
			atomic.StoreInt32(&c.State, 1)
			for i := 0; i < c.NumReduceTask; i++ {
				c.ReduceTask <- Task{IDReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if lenFinishTask(&c.ReduceTaskTime) != c.NumReduceTask {
		start_time, _ := c.ReduceTaskTime.Load(args.IDMap)
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.ReduceTaskTime.Store(args.IDReduce, TimeStamp{time_now, true})
		if lenFinishTask(&c.ReduceTaskTime) == c.NumReduceTask {
			atomic.StoreInt32(&c.State, 2)
		}
	}
	return nil
}

func (c *Coordinator) TimeTick() {
	state := atomic.LoadInt32(&c.State)
	time_now := time.Now().Unix()

	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {
			tmp, _ := c.MapTaskTime.Load(i)
			if !tmp.(TimeStamp).Finish && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("map crash")
				c.MapTask <- Task{FileName: c.files[i], IDMap: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
			tmp, _ := c.ReduceTaskTime.Load(i)
			if !tmp.(TimeStamp).Finish && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("worker crash")
				c.ReduceTask <- Task{IDReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	}
}

func (c *Coordinator) EndTask(args *TaskRequest, reply *TaskReply) {
	// if len(c.ReduceTaskFinish) == c.NumReduceTask {
	// 	return 1, nil
	// } else {
	// 	return 0, nil
	// }
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
	c.TimeTick()
	ret := false

	if lenFinishTask(&c.ReduceTaskTime) == c.NumReduceTask {
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
		State:         0,
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		files:         files,
	}

	time_now := time.Now().Unix()
	for id, file := range files {
		c.MapTask <- Task{FileName: file, IDMap: id}
		c.MapTaskTime.Store(id, TimeStamp{time_now, false})
	}

	// Your code here.

	c.server()
	return &c
}
