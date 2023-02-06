package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		reply := TaskReply{}
		args := TaskArgs{}
		CallGetTask(&args, &reply)
		filename := reply.XTask.FileName
		// MapID := reply.MapID
		// ReduceID := reply.ReduceID
		state := reply.XTask.TaskType
		// fmt.Println(">><<<<<<<<<<<<<<<<< state: ",state)
		// if reply.FileName == "" && reply.XTask.TaskType == 0 {
		// 	fmt.Println("><>>>>>>>>>>>>>>>>>>> ",reply.NumMapTask,reply.XTask.MapID)
		// }
		if state == 0 && reply.CurMapNum >= 0 {
			nReduce := reply.NumReduceTask
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open file %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))

			bucket := make([][]KeyValue, nReduce)
			for _, kv := range kva {
				num := ihash(kv.Key) % nReduce
				bucket[num] = append(bucket[num], kv)

			}
			for i := 0; i < nReduce; i++ {
				tempFile, err := ioutil.TempFile("", "mr-map-*"+strconv.Itoa(i))
				if err != nil {
					log.Fatalf("Could not create temporary file")
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range bucket[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Could not encode")
					}
				}
				os.Rename(tempFile.Name(), "mr-"+strconv.Itoa(reply.XTask.MapID)+"-"+strconv.Itoa(i))
				tempFile.Close()
			}
			CallFinishTask(&reply.XTask, &reply)
		} else if state == 1 && reply.CurReduceNum >= 0 {
			NumMapTask := reply.NumMapTask
			ReduceID := reply.XTask.ReduceID
			intermediate := []KeyValue{}

			for i := 0; i < NumMapTask; i++ {
				intermediateName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(ReduceID)
				intermediateFile, err := os.Open(intermediateName)
				if err != nil {
					log.Fatalf("Could not open intermediate file")
				}
				dec := json.NewDecoder(intermediateFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			// if ReduceID == 0 {
			// 	fmt.Println("$$$$$$$$$$ using reduce id 0", ReduceID)
			// }
			sort.Sort(ByKey(intermediate))
			oname := "mr-temp-" + strconv.Itoa(ReduceID)
			tempReduceFile, err := ioutil.TempFile("", oname)
			if err != nil {
				log.Fatalf("Could not create temporary file")
			}

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

				fmt.Fprintf(tempReduceFile, "%v %v\n", intermediate[i].Key, output)
				i = j

			}
			tempReduceFile.Close()
			os.Rename(tempReduceFile.Name(), "mr-out-"+strconv.Itoa(ReduceID))
			CallFinishTask(&reply.XTask, &reply)
		} else if state == 2 {
			break
		}
		time.Sleep(time.Second)
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask(args *TaskArgs, reply *TaskReply) {
	// args := TaskArgs{}
	// args.Msg = "hello\n"
	// reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if !ok {
		fmt.Printf("call get task failed!\n")
	}
}
func CallFinishTask(args *Task, reply *TaskReply) {
	ok := call("Coordinator.FinishTask", &args, &reply)

	if !ok {
		fmt.Printf("call finish failed!\n")
	}
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
