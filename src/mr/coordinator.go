package mr

import (
<<<<<<< HEAD
=======
	"fmt"
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
<<<<<<< HEAD

type Coordinator struct {
	// Your definitions here.
	// State            int // 0 map 1 reduce 2 finish
	State          int32
	MapTask        chan Task
	ReduceTask     chan Task
	NumMapTask     int
	NumReduceTask  int
	MapTaskTime    sync.Map
	ReduceTaskTime sync.Map
	// MapTaskFinish    chan bool
	// ReduceTaskFinish chan bool
	files []string
	Mu    sync.RWMutex
}

type Task struct {
	FileName string
	MapID    int
	ReduceID int
	TaskType int32
	// TaskTime TimeStamp
}

type TimeStamp struct {
	TimeNow int64
	Finish  bool
=======

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
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
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

<<<<<<< HEAD
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
=======
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	state := atomic.LoadInt32(&c.State)
	if state == 0 {
		if len(c.MapTask) != 0 {
			mapTask, ok := <-c.MapTask
			if ok {
				reply.XTask = mapTask
<<<<<<< HEAD
				c.MapTaskTime.Store(reply.XTask.MapID, TimeStamp{time.Now().Unix(), false})
			}
			reply.CurMapNum = len(c.MapTask)
		} else {
			reply.CurMapNum = -1
		}
		reply.CurReduceNum = len(c.ReduceTask)
=======
			}
			reply.CurNumMapTask = len(c.MapTask)
		} else {
			reply.CurNumMapTask = -1
		}
		reply.CurNumReduceTask = len(c.ReduceTask)
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	} else if state == 1 {
		if len(c.ReduceTask) != 0 {
			reduceTask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reduceTask
<<<<<<< HEAD
				c.ReduceTaskTime.Store(reply.XTask.ReduceID, TimeStamp{time.Now().Unix(), false})
			}
			reply.CurReduceNum = len(c.ReduceTask)
		} else {
			reply.CurReduceNum = -1
		}
		reply.CurMapNum = -1
	}

	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.XTask.TaskType = state
	return nil
}

func (c *Coordinator) FinishTask(args *Task, reply *TaskReply) error {
	timeNow := time.Now().Unix()

	if GetMapSize(&c.MapTaskTime) != c.NumMapTask {
		startTime, _ := c.MapTaskTime.Load(args.MapID)
		if timeNow-startTime.(TimeStamp).TimeNow > 10 {
			return nil
		}
		c.MapTaskTime.Store(args.MapID, TimeStamp{time.Now().Unix(), true})
		if GetMapSize(&c.MapTaskTime) == c.NumMapTask {
			atomic.StoreInt32(&c.State, 1)
			for i := 0; i < c.NumReduceTask; i++ {
				c.ReduceTask <- Task{ReduceID: i}
				// c.ReduceTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
			}
		}
	} else if GetMapSize(&c.ReduceTaskTime) != c.NumReduceTask {
		startTime, _ := c.ReduceTaskTime.Load(args.ReduceID)
		if timeNow-startTime.(TimeStamp).TimeNow > 10 {
			return nil
		}
		c.ReduceTaskTime.Store(args.ReduceID, TimeStamp{time.Now().Unix(), true})
		if GetMapSize(&c.ReduceTaskTime) == c.NumReduceTask {
			atomic.StoreInt32(&c.State, 2)
		}
	}

=======
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
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	return nil
}

func (c *Coordinator) TimeTick() {
	state := atomic.LoadInt32(&c.State)
<<<<<<< HEAD
	timeNow := time.Now().Unix()
	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {

			tempTask, ok := c.MapTaskTime.Load(i)
			if !ok {
				continue
			}
			if !tempTask.(TimeStamp).Finish && timeNow-tempTask.(TimeStamp).TimeNow > 10 {
				// fmt.Println("restart map:", i)
				c.MapTask <- Task{MapID: i, FileName: c.files[i]}
				c.MapTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
=======
	time_now := time.Now().Unix()

	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {
			tmp, _ := c.MapTaskTime.Load(i)
			if !tmp.(TimeStamp).Finish && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("map crash")
				c.MapTask <- Task{FileName: c.files[i], IDMap: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false})
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
<<<<<<< HEAD
			tempTask, ok := c.ReduceTaskTime.Load(i)
			if !ok {
				continue
			}
			if !tempTask.(TimeStamp).Finish && timeNow-tempTask.(TimeStamp).TimeNow > 10 {
				// fmt.Println("restart reduce:", i)
				c.ReduceTask <- Task{ReduceID: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
			}

=======
			tmp, _ := c.ReduceTaskTime.Load(i)
			if !tmp.(TimeStamp).Finish && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("worker crash")
				c.ReduceTask <- Task{IDReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
		}
	}
}

<<<<<<< HEAD
func GetMapSize(m *sync.Map) int {
	i := 0
	m.Range(func(key, value interface{}) bool {
		if value.(TimeStamp).Finish {
			i++
		}
		return true
	})
	return i
=======
func (c *Coordinator) EndTask(args *TaskRequest, reply *TaskReply) {
	// if len(c.ReduceTaskFinish) == c.NumReduceTask {
	// 	return 1, nil
	// } else {
	// 	return 0, nil
	// }
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
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
<<<<<<< HEAD
	// ret := true
=======
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	c.TimeTick()
	ret := false
	// fmt.Println("<<<<<<<<< ", GetMapSize(&c.ReduceTaskTime), c.NumReduceTask)

<<<<<<< HEAD
	if GetMapSize(&c.ReduceTaskTime) == c.NumReduceTask {
		ret = true
	}

	// time.Sleep(time.Second)
=======
	if lenFinishTask(&c.ReduceTaskTime) == c.NumReduceTask {
		ret = true
	}

>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
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
<<<<<<< HEAD
=======
		State:         0,
>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
<<<<<<< HEAD
		// MapTaskFinish:    make(chan bool, len(files)),
		// ReduceTaskFinish: make(chan bool, nReduce),
		files: files,
		State: 0,
	}

	for id, file := range files {
		c.MapTask <- Task{
			FileName: file,
			MapID:    id,
			TaskType: 0,
		}
		// c.MapTaskTime.Store(id, TimeStamp{time.Now().Unix(), false})

	}

=======
		files:         files,
	}

	time_now := time.Now().Unix()
	for id, file := range files {
		c.MapTask <- Task{FileName: file, IDMap: id}
		c.MapTaskTime.Store(id, TimeStamp{time_now, false})
	}

>>>>>>> bbdbefc2d2f7e8cc4afb11858717bb51a031aed2
	// Your code here.

	c.server()
	return &c
}
