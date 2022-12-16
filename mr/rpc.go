package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MAPTASK       = 1
	REDUCETASK    = 2
	ALLTASKFINISH = 3
	NOTASK        = 0
)

const (
	IDLE    = 1
	WORKING = 2
)
const (
	STATUS_OK    = 200
	STATUS_WRONG = 404
)

type RegisterWorkerParam struct {
	Address string `json:"address"`
}
type RegisterWorkerResponse struct {
	WorkerNumber int `json:"worker_number"`
}
type FetchTaskParam struct {
	WorkerNumber int `json:"worker_number"`
}
type FetchTaskResponse struct {
	Task Task `json:"task"`
}
type JobDoneParam struct {
	TaskType     int
	WorkerNumber int
	FileNames    []string
}
type JobDoneResponse struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
