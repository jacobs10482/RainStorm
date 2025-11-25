package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	rss "g51mp4/RainStormStructs"
)

// --------------------------
// Leader structures
// --------------------------

type AssignTaskArgs struct {
	TaskID int
	Stage  int
	Exe    string
	Args   []string
}


type WorkerInfo struct {
	Address string // worker RPC address
	Alive   bool
}

// --------------------------
// Leader state
// --------------------------

type Leader struct {
	mu          sync.Mutex
	workers     map[string]*WorkerInfo       // worker address → info
	taskMapping map[int]string               // taskID → worker address
	tupleBuffer map[int][]rss.Tuple              // taskID → tuples in-flight
}

// --------------------------
// RPC Methods for workers
// --------------------------


func (l *Leader) TaskFailed(args *rss.KillTaskArgs, reply *bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	workerAddr, ok := l.taskMapping[args.TaskID]
	if !ok {
		return fmt.Errorf("task %d not found in mapping", args.TaskID)
	}

	log.Printf("Task %d failed on worker %s, reassigning...\n", args.TaskID, workerAddr)

	// Save tuples for reassigning (in real implementation you may track in-flight tuples)
	tuples := l.tupleBuffer[args.TaskID]

	// Remove old mapping
	delete(l.taskMapping, args.TaskID)
	delete(l.tupleBuffer, args.TaskID)

	// Reassign task to a new worker (simplest: first alive worker)
	for addr, w := range l.workers {
		if w.Alive {
			log.Printf("Reassigning task %d to worker %s\n", args.TaskID, addr)
			go l.sendTask(addr, args.TaskID, tuples)
			break
		}
	}

	*reply = true
	return nil
}

// --------------------------
// Helper methods
// --------------------------

// Assign a task to a worker
func (l *Leader) sendTask(workerAddr string, taskID int, tuples []rss.Tuple) {
	client, err := rpc.Dial("tcp", workerAddr)
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v\n", workerAddr, err)
		return
	}
	defer client.Close()

	args := AssignTaskArgs{
		TaskID: taskID,
		Stage:  0,        // adjust stage as needed
		Exe:    "./op1",  // example, replace with real exe
		Args:   []string{"pattern"},
	}

	var reply bool
	if err := client.Call("Worker.AssignTask", &args, &reply); err != nil {
		log.Printf("Failed to assign task %d to worker %s: %v\n", taskID, workerAddr, err)
		return
	}

	if len(tuples) > 0 {
		addArgs := rss.AddTuplesArgs{
			TaskID: taskID,
			Tuples: tuples,
		}
		if err := client.Call("Worker.AddTuples", &addArgs, &reply); err != nil {
			log.Printf("Failed to send tuples to task %d on worker %s: %v\n", taskID, workerAddr, err)
		}
	}

	// Save mapping
	l.mu.Lock()
	l.taskMapping[taskID] = workerAddr
	l.tupleBuffer[taskID] = tuples
	l.mu.Unlock()
}

// --------------------------
// Main
// --------------------------

func main() {
	leader := &Leader{
		workers:     make(map[string]*WorkerInfo),
		taskMapping: make(map[int]string),
		tupleBuffer: make(map[int][]rss.Tuple),
	}

	// Example: register two workers
	leader.workers["127.0.0.1:9300"] = &WorkerInfo{Address: "127.0.0.1:9300", Alive: true}
	leader.workers["127.0.0.1:9301"] = &WorkerInfo{Address: "127.0.0.1:9301", Alive: true}

	rpc.Register(leader)
	ln, err := net.Listen("tcp", ":9400") // leader port
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Leader RPC listening on port 9400")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
