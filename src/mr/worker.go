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

	for {
		args := TaskRequest{}
		reply := TaskReply{}
		CallGetTask(&args, &reply)
		filename := reply.XTask.FileName
		state := reply.State
		if state == 0 {
			id := strconv.Itoa(reply.XTask.IDMap)
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

			bucket := make([][]KeyValue, reply.NumReduceTask)

			for _, kv := range kva {
				num := ihash(kv.Key) % reply.NumReduceTask
				bucket[num] = append(bucket[num], kv)
			}
			for i := 0; i < reply.NumReduceTask; i++ {
				sort.Sort(ByKey(bucket[i]))
				tempFile, err := ioutil.TempFile("", "mr-map-*")
				if err != nil {
					log.Fatalf("error creating temp file %v", tempFile.Name())
				}
				enc := json.NewEncoder(tempFile)

				err = enc.Encode(bucket[i])
				if err != nil {
					log.Fatalf("error encoding\n")
				}
				tempFile.Close()
				os.Rename(tempFile.Name(), "mr-"+id+"-"+strconv.Itoa(i))
			}
			CallFinishTask()
		} else if state == 1 {
			id := strconv.Itoa(reply.XTask.IDReduce)
			intermediate := []KeyValue{}
			// X := reply.XTask.IDMap
			for i := 0; i < reply.NumMapTask; i++ {
				mapFileName := "mr-" + strconv.Itoa(i) + "-" + id
				// inputFile, err := os.OpenFile(mapFileName, os.O_RDONLY, 0777)
				inputFile, err := os.Open(mapFileName)
				if err != nil {
					log.Fatalf("error opening map file: %v\n", mapFileName)
				}

				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					// var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					// intermediate = append(intermediate, kv)
					intermediate = append(intermediate, kv...)
				}
			}
			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + id
			tempReduceFile, err := ioutil.TempFile("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("Error creating temp file: %v", tempReduceFile)
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
			os.Rename(tempReduceFile.Name(), oname)

			CallFinishTask()
		} else {
			break
		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

func CallGetTask(args *TaskRequest, reply *TaskReply) {
	// call("Coordinator.GetTask", &args, &reply)
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Println("call get task ok")
	} else {
		fmt.Println("call get task failed")
	}
}

func CallFinishTask() {
	args := TaskRequest{}
	reply := TaskReply{}
	// call("Coordinator.FinishTask", &args, &reply)
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		fmt.Println("call finish task ok")
	} else {
		fmt.Println("call finish task failed")
	}
}

func CallEndTask() {
	args := TaskRequest{}
	reply := TaskReply{}
	// call("Coordinator.FinishTask", &args, &reply)
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		fmt.Println("call end task ok")
	} else {
		fmt.Println("call end task failed")
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
