package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"strings"
	"sync"
	rss "g51mp4/RainStormStructs"
	fd "g51mp4/failure_detection"
	hydfs "g51mp4/hydfs_system"
	"os"
)

var node *fd.Node

// --------------------------
// Task structures
// --------------------------

type TaskState struct {
	ID     int
	Stage  int
	HydfsDest   string
	LastStageOutputFile string
	Cmd    *exec.Cmd
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
	Downstream []rss.DownstreamInfo // IP:port of next stage task
}

// --------------------------
// Global task map
// --------------------------

var tasks = map[int]*TaskState{}
var tasksMu sync.Mutex

// --------------------------
// Worker RPC methods
// --------------------------

type Worker struct{}



func sendRPC(addr string, method string, args interface{}, reply interface{}) error {
    client, err := rpc.Dial("tcp", addr+":9300")
    if err != nil {
        return err
    }
    defer client.Close()
    return client.Call(method, args, reply)
}



func HashKey(key string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(key))
    return h.Sum32()
}

func parseTuple(line string) rss.Tuple {
    parts := strings.SplitN(line, "\t", 2) // split into at most 2 parts
    if len(parts) < 2 {
        return rss.Tuple{
            Key:   line,
            Value: "",
        }
    }
    return rss.Tuple{
        Key:   parts[0],
        Value: parts[1],
    }
}




func (w *Worker) AssignTask(args *rss.AssignTaskArgs, reply *bool) error {
	
	tasksMu.Lock()
	defer tasksMu.Unlock()

	argList := []string{}
	if strings.TrimSpace(args.Args) != "" {
		argList = strings.Fields(args.Args)
	}

	cmd := exec.Command(args.Exe, argList...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	ts := &TaskState{
		ID:     args.TaskID,
		Stage:  args.Stage,
		Cmd:    cmd,
		HydfsDest:   args.Dest,
		Stdin:  stdin,
		Stdout: stdout,
		Downstream: args.Downstream,
		LastStageOutputFile: fmt.Sprintf("last_stage_output_%d.txt", args.TaskID),
	}

	tasks[args.TaskID] = ts
	fmt.Printf("Assigned task: %s\n", args.Exe)
	// Start goroutine to read task output
	go func() {
    scanner := bufio.NewScanner(ts.Stdout)
    for scanner.Scan() {
        line := scanner.Text()

        tuple := parseTuple(line)

		if len(ts.Downstream) == 0 {
			// For now: print key/value to terminal
			fmt.Printf("%s\t%s\n", tuple.Key, tuple.Value)

			// Eventually: write to hydfs through your Node API.
			lineOutput := fmt.Sprintf("%s\t%s\n", tuple.Key, tuple.Value)
			os.WriteFile(ts.LastStageOutputFile, []byte(lineOutput), 0644)
			hydfs.HandleAppend(node, ts.LastStageOutputFile, ts.HydfsDest)
			os.WriteFile(ts.LastStageOutputFile, []byte{}, 0644)
			continue
		}


        idx := int(HashKey(tuple.Key)) % len(ts.Downstream)
        target := ts.Downstream[idx]

        // forward
        for {
			
			var dummyReply bool
			err := sendRPC(target.IP, "Worker.AddTuples",
				&rss.AddTuplesArgs{
					TaskID: target.TaskID,
					Tuples: []rss.Tuple{tuple},
				},
				&dummyReply,
			)
			if err == nil {
				break // success, move on to next tuple
			}
			
			// RPC failed → notify leader
			//notifyLeaderTaskFailed(ts.ID)

			// wait for leader to push new downstream info
			//time.Sleep(100 * time.Millisecond)
			//idx := int(HashKey(tuple.Key)) % len(ts.Downstream) // recompute new target				//NEED TO DEAL WITH TASK FAILURE + RPC FAILURE (SCANNER FAILS)
			//target = ts.Downstream[idx]
		}

    }
}()


	*reply = true
	return nil
}

func (w *Worker) AddTuples(args *rss.AddTuplesArgs, reply *bool) error {
    tasksMu.Lock()
    ts, ok := tasks[args.TaskID]
    tasksMu.Unlock()

    if !ok {
        return fmt.Errorf("task %d not found", args.TaskID)
    }

    // FINAL STAGE — no downstream
    if len(ts.Downstream) == 0 {
        // Write the tuples to the operator process
        for _, t := range args.Tuples {
            fmt.Fprintf(ts.Stdin, "%s\t%s\n", t.Key, t.Value)
        }
        *reply = true
        return nil
    }

    // NON-FINAL STAGES
    for _, t := range args.Tuples {
        fmt.Fprintf(ts.Stdin, "%s\t%s\n", t.Key, t.Value)
    }

    *reply = true
    return nil
}



func (w *Worker) KillTask(args *rss.KillTaskArgs, reply *bool) error {
	tasksMu.Lock()
	ts, ok := tasks[args.TaskID]
	if ok {
		delete(tasks, args.TaskID)
	}
	tasksMu.Unlock()
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	ts.Cmd.Process.Kill()
	ts.Stdin.Close()
	ts.Stdout.Close()

	*reply = true
	return nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal("Error getting addresses:", err)
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil && !ipnet.IP.IsLoopback() {
			return ipnet.IP.String()
		}
	}
	return ""
}

func (w *Worker) Heartbeat(_ *struct{}, reply *bool) error {
    *reply = true
    return nil
}
// --------------------------
// Main RPC listener
// --------------------------

func main() {
	
	node = hydfs.Start()

	ip := getLocalIP()


	port := "9300" // Or read from command-line arguments

	worker := &Worker{}
	rpc.Register(worker)

	ln, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Worker RPC listening on port", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
