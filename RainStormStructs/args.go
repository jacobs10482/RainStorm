package rainstormrpc

type Tuple struct {
    Key   string
    Value string
}

type AssignTaskArgs struct {
    TaskID        int
    Stage         int
    Exe           string
    Args          []string
    Downstream []DownstreamInfo
}

type DownstreamInfo struct {
    TaskID    int
    IP    string  // "ip:port"
}

type AddTuplesArgs struct {
    TaskID int
    Tuples []Tuple
}

type KillTaskArgs struct {
    TaskID int
}

type TupleOutputArgs struct {
    TaskID int
    Stage  int
    Tuple  Tuple
}

