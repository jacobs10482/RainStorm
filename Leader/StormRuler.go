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
	fd "g51mp4/failure_detection"
	hydfs "g51mp4/hydfs_system"
)

var node *fd.Node


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



func sendRPC(addr string, method string, args interface{}, reply interface{}) error {
    client, err := rpc.Dial("tcp", addr+":9300")
    if err != nil {
        return err
    }
    defer client.Close()
    return client.Call(method, args, reply)
}


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

func taskID(stage int, index int, tasksPerStage int) int {
    return stage * tasksPerStage + index
}


func (l *Leader) assignAllTasks(cmd *RainStormCommand) error {
    workers := l.workers
    if len(workers) == 0 {
        return fmt.Errorf("no workers available")
    }

    wcount := len(workers)
    tps := cmd.NtasksPerStage

    // Precompute worker assignment for every task
    taskToWorker := make(map[int]string)

    for stage := 0; stage < cmd.Nstages; stage++ {
        for i := 0; i < tps; i++ {
            tid := taskID(stage, i, tps)
            worker := workers[tid % wcount]  // round-robin
            taskToWorker[tid] = worker
        }
    }

    // Now send AssignTask RPC for each task
    for stage := 0; stage < cmd.Nstages; stage++ {
        for i := 0; i < tps; i++ {
            tid := taskID(stage, i, tps)
            worker := taskToWorker[tid]

            // Build downstream list
            var downstream []rss.DownstreamInfo
            if stage < cmd.Nstages-1 {
                nextStage := stage + 1
                for j := 0; j < tps; j++ {
                    dtid := taskID(nextStage, j, tps)
                    downstream = append(downstream, rss.DownstreamInfo{
                        IP:     taskToWorker[dtid],
                        TaskID: dtid,
                    })
                }
            }

            // Build RPC args
            args := &rss.AssignTaskArgs{
                TaskID:    tid,
                Stage:     stage,
				Dest:      cmd.HydfsDest,
                Exe:       cmd.Ops[stage].Exe,
                Args:      cmd.Ops[stage].Args,
                Downstream: downstream,
            }

            // Perform RPC
            var reply bool
            if err := sendRPC(worker, "Worker.AssignTask", args, &reply); err != nil {
                return fmt.Errorf("assigning task %d to %s failed: %v", tid, worker, err)
            }

            // Save mapping
            l.taskMapping[tid] = worker
        }
    }

    return nil
}

func (l *Leader) ReadFileAndSendTuples(filename string, nTasksStage1 int) error {
    file, err := os.Open(filename)
    if err != nil {
        return fmt.Errorf("failed to open file %s: %v", filename, err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    lineNum := 0
    for scanner.Scan() {
        line := scanner.Text()
        key := fmt.Sprintf("%s:%d", filename, lineNum)
        tuple := rss.Tuple{
            Key:   key,
            Value: line,
        }

        // Hash key to pick stage 1 task
        taskIdx := int(rss.HashKey(tuple.Key)) % nTasksStage1
        taskID := taskID(0, taskIdx, nTasksStage1) // assuming stage 0 = first stage
        workerAddr, ok := l.taskMapping[taskID]
        if !ok {
            return fmt.Errorf("no worker assigned for task %d", taskID)
        }

        // Send the tuple via RPC
        args := rss.AddTuplesArgs{
            TaskID: taskID,
            Tuples: []rss.Tuple{tuple},
        }
        var reply bool
        if err := sendRPC(workerAddr, "Worker.AddTuples", &args, &reply); err != nil {
            log.Printf("failed to send tuple to worker %s task %d: %v", workerAddr, taskID, err)
            // optionally buffer for retry
        }

        lineNum++
    }

    if err := scanner.Err(); err != nil {
        return fmt.Errorf("error reading file %s: %v", filename, err)
    }

    return nil
}




// --------------------------
// Main
// --------------------------

func main() {

	node = hydfs.Start()


	
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

		hydfs.HandleCreate(node, "emptyfile.txt", cmd.HydfsDest)
		leader.assignAllTasks(cmd)



		// TODO: pass cmd to RainStorm start logic
		fmt.Println("Command processed. Enter next command:")
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}

}
