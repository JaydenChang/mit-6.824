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

type ByKey []KeyValue

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
		args := TaskResponse{}
		reply := TaskReply{}
		CallGetTask(&args, &reply)
		bucket := make([][]KeyValue, reply.NumReduceTask)
		filename := reply.ReplyTask.FileName
		if filename != "" {
			fmt.Println(">>>> map task")
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
			id := strconv.Itoa(reply.ReplyTask.IDMap)
			for _, kv := range kva {
				num := ihash(kv.Key) % reply.NumReduceTask
				bucket[num] = append(bucket[num], kv)
			}
			for i := 0; i < reply.NumReduceTask; i++ {
				tempFile, err := ioutil.TempFile("", "mr-app-*")
				if err != nil {
					log.Fatalf("Could not create tempFile")
				}
				enc := json.NewEncoder(tempFile)
				err1 := enc.Encode(bucket[i])
				if err1 != nil {
					log.Fatal("encode fail")
				}
				tempFile.Close()
				outFileName := "mr-" + id + "-" + strconv.Itoa(i)
				os.Rename(tempFile.Name(), outFileName)
			}
			reply.MapTaskFinish <- true
		} else {
			fmt.Println(">>>>>>>>>> reduce task")
			id := strconv.Itoa(reply.ReplyTask.IDMap)
			if len(reply.MapTaskFinish) == reply.NumMapTask {
				fmt.Println(">>>>>>>> reduce task start")
				intermediate := []KeyValue{}
				for i := 0; i < reply.NumMapTask; i++ {
					mapFileName := "mr-" + strconv.Itoa(i) + "-" + id
					file, err := os.Open(mapFileName)
					if err != nil {
						log.Fatalf("Cannot open the reduce task %v", mapFileName)
					}
					dnc := json.NewDecoder(file)
					for {
						var kv []KeyValue
						if err := dnc.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv...)
					}
				}
				sort.Sort(ByKey(intermediate))

				outFile := "mr-out-" + id
				tmpFile, err := ioutil.TempFile("", "mr-reduce-*")
				if err != nil {
					log.Fatalf("Could not create temporary file: %v", tmpFile)
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

					fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				tmpFile.Close()
				os.Rename(tmpFile.Name(), outFile)
			}
			reply.ReduceTaskFinish <- true
		}

		if len(reply.ReduceTaskFinish) == reply.NumReduceTask {
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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask(args *TaskResponse, reply *TaskReply) {
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		// fmt.Printf("reply.Y %v\n", reply.ReplyTask.FileName)
		fmt.Println("call get task ok")
	} else {
		fmt.Printf("call failed!\n")
	}
	// if !ok {
	// 	fmt.Printf("call failed!\n")
	// }
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
