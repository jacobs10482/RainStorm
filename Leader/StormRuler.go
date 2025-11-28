package main

import (
	"bufio"
	"fmt"
	rss "g51mp4/RainStormStructs"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

var vmMapRPC = map[string]string{
    "vm1":  "172.22.95.98:9200",
    "vm2":  "172.22.154.169:9200",
    "vm3":  "172.22.158.169:9200",
    "vm4":  "172.22.95.99:9200",
    "vm5":  "172.22.154.170:9200",
    "vm6":  "172.22.158.170:9200",
    "vm7":  "172.22.95.100:9200",
    "vm8":  "172.22.154.171:9200",
    "vm9":  "172.22.158.171:9200",
    "vm10": "172.22.95.101:9200",
}

type RainStormCommand struct {
	Nstages         int
	NtasksPerStage  int
	Ops             []StageOp
	HydfsSrc        string
	HydfsDest       string
	ExactlyOnce     bool
	Autoscale       bool
	InputRate       int
	LW              int
	HW              int
}

type StageOp struct {
	Exe  string
	Args string
}


// --------------------------
// Leader structures
// --------------------------

type AssignTaskArgs struct {
	TaskID int
	Stage  int
	Exe    string
	Args   []string
}


// --------------------------
// Leader state
// --------------------------

type Leader struct {
    mu          sync.Mutex
    workers     []string    // set of alive workers
    taskMapping map[int]string         // taskID → worker addr
    tupleBuffer map[int][]rss.Tuple
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
	for _, addr := range l.workers {
		log.Printf("Reassigning task %d to worker %s\n", args.TaskID, addr)
		go l.sendTask(addr, args.TaskID, tuples)
		break
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

func discoverWorkers() []string {
    live := []string{}
    for _, addr := range vmMapRPC {
        if pingWorker(addr) {
            live = append(live, addr)
        }
    }
    return live
}

func pingWorker(addr string) bool {
    client, err := rpc.Dial("tcp", addr)
    if err != nil {
        return false
    }
    defer client.Close()

    var ok bool
    callErr := client.Call("Worker.Heartbeat", &struct{}{}, &ok)
    return callErr == nil && ok
}



func parseRainStormCommand(line string) (*RainStormCommand, error) {
	parts := strings.Fields(line)
	if len(parts) < 7 { // minimal check
		return nil, fmt.Errorf("not enough arguments")
	}

	if parts[0] != "RainStorm" {
		return nil, fmt.Errorf("command must start with 'RainStorm'")
	}

	// Parse stages and tasks
	nStages, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid Nstages: %v", err)
	}

	nTasks, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid Ntasks_per_stage: %v", err)
	}

	if len(parts) < 3+2*nStages+5 { // 2 per stage + hydfs src/dest + exactly_once + autoscale + 3 optional
		return nil, fmt.Errorf("not enough arguments for stages and parameters")
	}

	ops := make([]StageOp, nStages)
	for i := 0; i < nStages; i++ {
		exe := parts[3+i*2]
		arg := parts[3+i*2+1]
		// remove quotes if present
		arg = strings.Trim(arg, "\"")
		ops[i] = StageOp{
			Exe:  exe,
			Args: arg,
		}
	}

	offset := 3 + 2*nStages
	hydfsSrc := parts[offset]
	hydfsDest := parts[offset+1]

	exactlyOnce, err := strconv.ParseBool(parts[offset+2])
	if err != nil {
		return nil, fmt.Errorf("invalid exactly_once: %v", err)
	}

	autoscale, err := strconv.ParseBool(parts[offset+3])
	if err != nil {
		return nil, fmt.Errorf("invalid autoscale_enabled: %v", err)
	}

	inputRate, lw, hw := 0, 0, 0
	if autoscale {
		inputRate, err = strconv.Atoi(parts[offset+4])
		if err != nil {
			return nil, fmt.Errorf("invalid INPUT_RATE: %v", err)
		}
		lw, err = strconv.Atoi(parts[offset+5])
		if err != nil {
			return nil, fmt.Errorf("invalid LW: %v", err)
		}
		hw, err = strconv.Atoi(parts[offset+6])
		if err != nil {
			return nil, fmt.Errorf("invalid HW: %v", err)
		}
	}

	return &RainStormCommand{
		Nstages:        nStages,
		NtasksPerStage: nTasks,
		Ops:            ops,
		HydfsSrc:       hydfsSrc,
		HydfsDest:      hydfsDest,
		ExactlyOnce:    exactlyOnce,
		Autoscale:      autoscale,
		InputRate:      inputRate,
		LW:             lw,
		HW:             hw,
	}, nil
}


// --------------------------
// Main
// --------------------------

func main() {
	leader := &Leader{
		workers:     discoverWorkers(),         // list of available VMs
		taskMapping: make(map[int]string),      // taskID → worker
		tupleBuffer: make(map[int][]rss.Tuple), // in-flight tuples
	}

	// Register leader RPC
	rpc.Register(leader)

	// Start listening for RPC connections
	ln, err := net.Listen("tcp", ":9300") // leader port
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Leader RPC listening on port 9300")

	// RPC listener in its own goroutine
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// CLI input loop in main goroutine
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Leader ready. Enter RainStorm command:")

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		cmd, err := parseRainStormCommand(line)
		if err != nil {
			fmt.Println("Error parsing command:", err)
			continue
		}

		fmt.Printf("Parsed command: %+v\n", cmd)

		// TODO: pass cmd to RainStorm start logic
		fmt.Println("Command processed. Enter next command:")
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}

}
