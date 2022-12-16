package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	workerNumber int
	Mapf         func(string, string) []KeyValue
	Reducef      func(string, []string) string
	allTaskDone  = make(chan struct{}, 0)
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RegisterWorker() {
	registerRequest := RegisterWorkerParam{}
	registerWorkerResponse := RegisterWorkerResponse{}
	sockname := "/var/tmp/workernode" + uuid.New().String()
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("deamon agent cannot establish at %v, error:%v", sockname, err.Error())
	}
	registerRequest.Address = sockname
	err = call("Coordinator.RegisterWorker", &registerRequest, &registerWorkerResponse)
	if err != nil {
		log.Fatalf("cannot register as a worker %v", err.Error())
	}
	workerNumber = registerWorkerResponse.WorkerNumber
	go func() {
		log.Println("worker deamon is listening on %v,workerNumber is %v", sockname, registerWorkerResponse.WorkerNumber)
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf(err.Error())
			}
			log.Println("heartTickFromServer %v", conn.LocalAddr().String())
		}
	}()

}

func doMapTask(task Task) {
	filename := task.FileNames[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open file,filename: %v,error: %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read from %v,error: %v", filename, err)
	}
	file.Close()
	intermediate := []KeyValue{}
	kva := Mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	files := []*os.File{}
	for i := 0; i < 10; i++ {
		file, err = ioutil.TempFile("./", "tempMap")
		files = append(files, file)
	}
	for _, v := range intermediate {
		hashV := ihash(v.Key) % task.ReduceCount
		enc := json.NewEncoder(files[hashV])
		enc.Encode(v)
	}
	filenames := make([]string, 0)
	for k, v := range files {
		v.Close()
		filename := "./" + "mr-map-" + strconv.Itoa(task.TaskNo) + "-" + strconv.Itoa(k)
		os.Rename(v.Name(), filename)
		filenames = append(filenames, filename)
	}
	jobDoneParam := JobDoneParam{
		TaskType:     MAPTASK,
		FileNames:    filenames,
		WorkerNumber: workerNumber,
	}
	jobDoneResponse := JobDoneResponse{}
	err = call("Coordinator.TaskIsDone", &jobDoneParam, &jobDoneResponse)

}
func doReduceTask(task Task) {
	oname := "./mr-out-" + strconv.Itoa(task.TaskNo)
	ofile, _ := os.CreateTemp("./", "reducetemp")
	kva := []KeyValue{}
	for _, v := range task.FileNames {
		file, err := os.Open(v)
		if err != nil {
			log.Printf("cannot open file %v \n", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := Reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	filenames := make([]string, 0)
	filenames = append(filenames, oname)
	jobDoneParam := JobDoneParam{
		TaskType:     REDUCETASK,
		FileNames:    filenames,
		WorkerNumber: workerNumber,
	}

	jobDoneResponse := JobDoneResponse{}
	call("Coordinator.TaskIsDone", &jobDoneParam, &jobDoneResponse)

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	Mapf = mapf
	Reducef = reducef
	RegisterWorker()
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for {
			<-ticker.C
			param := FetchTaskParam{
				WorkerNumber: workerNumber,
			}
			response := FetchTaskResponse{}
			err := call("Coordinator.FetchTask", &param, &response)
			if err != nil {
				log.Println("error fetching task , error: ", err.Error())
			}
			if response.Task.TaskType == MAPTASK {
				log.Printf("worker %v is doing Map Task ,fileName = %v", workerNumber, response.Task.FileNames)
				doMapTask(response.Task)
			} else if response.Task.TaskType == REDUCETASK {
				log.Printf("worker %v is doing reduce Task ,fileName = %v", workerNumber, response.Task.FileNames)
				doReduceTask(response.Task)
			} else if response.Task.TaskType == ALLTASKFINISH {
				close(allTaskDone)
			}
		}
	}()
	<-allTaskDone
	log.Printf("Worker %v is exiting\n", workerNumber)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallATask() *TaskResponse {

// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
