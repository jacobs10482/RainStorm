package rainstormrpc

import "hash/fnv"


func HashKey(key string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(key))
    return h.Sum32()
}


type Tuple struct {
    Key   string
    Value string
    //TupleID int64
}

type AssignTaskArgs struct {
    TaskID        int
    Stage         int
    Exe           string
    Args          string
    Dest          string
    Downstream []DownstreamInfo
    Exactly_Once   bool
    Autoscale_Enabled bool
    InputRate   int
    LW          int
    HW          int
}

type DownstreamInfo struct {
    TaskID    int
    IP    string  // "ip:port"
}

type AddTuplesArgs struct {
    TaskID int
    Tuples []Tuple
    SourceIP   string
    SourceTask int
}

type ReviveTaskArgs struct {
    Args    *AssignTaskArgs
}

type KillTaskArgs struct {
    TaskID    int
}

type TupleOutputArgs struct {
    TaskID int
    Stage  int
    Tuple  Tuple
}

type UpdateDownstreamArgs struct {
    TaskID     int
    Downstream DownstreamInfo
}


type TaskIPAndPID struct {
    IP  string
    PID int
}