package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

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

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	state := atomic.LoadInt32(&c.State)
	if state == 0 {
		if len(c.MapTask) != 0 {
			mapTask, ok := <-c.MapTask
			if ok {
				reply.XTask = mapTask
			}
			reply.CurMapNum = len(c.MapTask)
		} else {
			reply.CurMapNum = -1
		}
		reply.CurReduceNum = len(c.ReduceTask)
	} else if state == 1 {
		if len(c.ReduceTask) != 0 {
			reduceTask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reduceTask
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
				c.ReduceTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
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

	return nil
}

func (c *Coordinator) TimeTick() {
	state := atomic.LoadInt32(&c.State)
	timeNow := time.Now().Unix()
	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {

			tempTask, _ := c.MapTaskTime.Load(i)
			if !tempTask.(TimeStamp).Finish && timeNow-tempTask.(TimeStamp).TimeNow > 10 {
				c.MapTask <- Task{MapID: i, FileName: c.files[i]}
				c.MapTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
			tempTask, _ := c.ReduceTaskTime.Load(i)
			if !tempTask.(TimeStamp).Finish && timeNow-tempTask.(TimeStamp).TimeNow > 10 {
				c.ReduceTask <- Task{ReduceID: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time.Now().Unix(), false})
			}

		}
	}
}

func GetMapSize(m *sync.Map) int {
	i := 0
	m.Range(func(key, value interface{}) bool {
		if value.(TimeStamp).Finish {
			i++
		}
		return true
	})
	return i
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
	// ret := true
	c.TimeTick()
	ret := false
	// fmt.Println("<<<<<<<<< ", GetMapSize(&c.ReduceTaskTime), c.NumReduceTask)

	if GetMapSize(&c.ReduceTaskTime) == c.NumReduceTask {
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
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
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
		c.MapTaskTime.Store(id, TimeStamp{time.Now().Unix(), false})

	}

	// Your code here.

	c.server()
	return &c
}
