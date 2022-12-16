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

var (
	workerNum         int
	workerMap         sync.Map
	mu                sync.Mutex
	currentTasks      = make(chan Task, 500)
	reduceFileNames   [][]string
	phase             int
	taskDone          atomic.Bool
	currentworkerNums atomic.Int32
)

type Task struct {
	TaskType    int      `json:"task_type"`
	TaskNo      int      `json:"task_no"`
	FileNames   []string `json:"file_name"`
	ReduceCount int      `json:reduce_count`
}

type worker struct {
	sockname string
	status   int
	task     Task
}
type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) FetchTask(request *FetchTaskParam, response *FetchTaskResponse) error {
	if taskDone.Load() == true {
		response.Task.TaskType = ALLTASKFINISH
		return nil
	}
	if len(currentTasks) == 0 {
		return nil
	}
	workerNumber := request.WorkerNumber
	workertmp, ok := workerMap.Load(workerNumber)
	if !ok {
		log.Printf("worker %v does not exist! \n", workerNumber)
		return nil
	}
	worker := workertmp.(worker)
	newTask := <-currentTasks
	worker.task = newTask
	worker.status = WORKING
	workerMap.Store(workerNumber, worker)
	response.Task = newTask
	return nil
}

func (c *Coordinator) TaskIsDone(request *JobDoneParam, response *JobDoneResponse) error {
	switch request.TaskType {
	case MAPTASK:
		for k, v := range request.FileNames {
			reduceFileNames[k] = append(reduceFileNames[k], v)
		}
		workerNumber := request.WorkerNumber
		workertmp, ok := workerMap.Load(workerNumber)
		if !ok {
			log.Printf("worker %v does not exist! \n", workerNumber)
			return nil
		}
		worker := workertmp.(worker)
		log.Printf("work done worker Id %v ,task type MAP id: %v \n", workerNumber, worker.task.TaskNo)
		worker.status = IDLE
		workerMap.Store(workerNumber, worker)
		break
	case REDUCETASK:
		for _, v := range request.FileNames {
			log.Printf("reducetask is down file is %v \n", v)
		}
		workerNumber := request.WorkerNumber
		workertmp, ok := workerMap.Load(workerNumber)
		if !ok {
			log.Printf("worker %v does not exist! \n", workerNumber)
			return nil
		}
		worker := workertmp.(worker)
		worker.status = IDLE
		workerMap.Store(workerNumber, worker)
		break

	}
	return nil
}
func (c *Coordinator) RegisterWorker(request *RegisterWorkerParam, response *RegisterWorkerResponse) error {
	w := worker{
		sockname: request.Address,
		status:   IDLE,
	}
	mu.Lock()
	//time.Sleep(time.Second)
	response.WorkerNumber = workerNum
	workerNum++
	mu.Unlock()
	workerMap.Store(response.WorkerNumber, w)
	log.Println("New worker Coming!!", w.sockname, w.status, response.WorkerNumber)
	currentworkerNums.Add(1)
	return nil
}
func clientHeartTick() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			<-ticker.C
			idle := true
			workerMap.Range(func(key, value interface{}) bool {
				failTimes := 0
				workerNumber := key.(int)
				worker := value.(worker)
				workerAddr, _ := net.ResolveUnixAddr("unix", worker.sockname)
				for failTimes < 3 {
					listner, err := net.DialUnix("unix", nil, workerAddr)
					if err != nil {
						failTimes++
						continue
					}
					if worker.status == WORKING {
						idle = false
					}

					listner.Close()
					return true
				}
				currentworkerNums.Add(-1)
				workerMap.Delete(key)
				log.Printf("worker num %v, sockname %v isdown! \n", workerNumber, worker.sockname)
				if worker.status == WORKING && worker.task.TaskType != NOTASK {
					currentTasks <- worker.task
				}
				return true
			})
			if idle && len(currentTasks) == 0 {
				if phase == 0 {
					taskNo := 0
					for _, v := range reduceFileNames {
						newTask := Task{
							TaskType:  REDUCETASK,
							TaskNo:    taskNo,
							FileNames: v,
						}
						taskNo++
						currentTasks <- newTask
					}
					phase = 1
				} else {
					taskDone.Store(true)
				}
			}
		}
	}()
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return currentworkerNums.Load() == 0 && taskDone.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	reduceFileNames = make([][]string, nReduce)
	// Your code here.
	taskDone.Store(false)
	currentworkerNums.Store(0)
	c.server()
	clientHeartTick()
	tasknum := 0
	for _, v := range files {
		newTask := Task{
			TaskType:    MAPTASK,
			FileNames:   make([]string, 0),
			TaskNo:      tasknum,
			ReduceCount: nReduce,
		}
		newTask.FileNames = append(newTask.FileNames, v)
		currentTasks <- newTask
		tasknum++
	}
	return &c
}
