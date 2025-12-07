package rainstormrpc

import "hash/fnv"

func HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

type Tuple struct {
	TupleID string // Unique ID: "taskID:seqNum" or "leader:lineNum"
	Key     string
	Value   string
}

type AssignTaskArgs struct {
	TaskID            int
	Stage             int
	Exe               string
	Args              string
	Dest              string
	Downstream        []DownstreamInfo
	LeaderIP          string
	Exactly_Once      bool
	Autoscale_Enabled bool
	InputRate         int
	LW                int
	HW                int
}

type DownstreamInfo struct {
	TaskID int
	IP     string // "ip:port"
}

type AddTuplesArgs struct {
	TaskID     int
	Tuples     []Tuple
	SourceIP   string
	SourceTask int
}

type ReviveTaskArgs struct {
	Args *AssignTaskArgs
}

type KillTaskArgs struct {
	TaskID int
}

type TupleOutputArgs struct {
	TaskID int
	Stage  int
	Tuple  Tuple
}

type UpdateDownstreamArgs struct {
	TaskID          int
	DownstreamIndex int // index in the Downstream slice to update
	Downstream      DownstreamInfo
}

type TaskIPAndPID struct {
	IP  string
	PID int
}

type TaskMetric struct {
	TaskID int
	Stage  int
	Rate   float64
	Worker string
}

type MetricsArgs struct {
	Metrics []TaskMetric
}

// In RainStormStructs package

type TaskReport struct {
	TaskID  int
	PID     int
	Exe     string
	LogFile string
}

type GetTaskStatusArgs struct {
	// Empty, we just want everything
}

type GetTaskStatusReply struct {
	Reports []TaskReport
}

// Request to get the current length of a task's input queue on the worker.
type GetQueueLenArgs struct {
	TaskID int
}

type QueueLenReply struct {
	Length int
}
