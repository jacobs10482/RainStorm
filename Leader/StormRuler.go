package main

import (
	"bufio"
	"fmt"
	rss "g51mp4/RainStormStructs"
	fd "g51mp4/failure_detection"
	hydfs "g51mp4/hydfs_system"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var node *fd.Node

var vmMapRPC = map[string]string{
	"vm1":  "172.22.95.98:9300",
	"vm2":  "172.22.154.169:9300",
	"vm3":  "172.22.158.169:9300",
	"vm4":  "172.22.95.99:9300",
	"vm5":  "172.22.154.170:9300",
	"vm6":  "172.22.158.170:9300",
	"vm7":  "172.22.95.100:9300",
	"vm8":  "172.22.154.171:9300",
	"vm9":  "172.22.158.171:9300",
	"vm10": "172.22.95.101:9300",
}

var tasksPerStage int = 0

type RainStormCommand struct {
	Nstages        int
	NtasksPerStage int
	Ops            []StageOp
	HydfsSrc       string
	HydfsDest      string
	ExactlyOnce    bool
	Autoscale      bool
	InputRate      int
	LW             int
	HW             int
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
	mu              sync.Mutex
	workers         []string                 // set of alive workers
	taskMapping     map[int]rss.TaskIPAndPID // taskID → worker addr
	tupleBuffer     map[int][]rss.Tuple
	ackedTuples     map[string]rss.Tuple // Track acked tuples from stage 1
	ackedMu         sync.RWMutex
	sentTuples      map[string]rss.Tuple // Track tuples sent by leader (for resends)
	sentTask        map[string]int       // mapping tuple key -> taskID it was sent to
	sentMu          sync.RWMutex
	ExactlyOnce     bool
	RoundRobinIndex int
}

// --------------------------
// RPC Methods for Leader
// --------------------------

func (l *Leader) AckTuple(args *rss.TupleOutputArgs, reply *bool) error {
	// Leader receives acks from first stage tasks
	l.ackedMu.Lock()
	l.ackedTuples[args.Tuple.Key] = args.Tuple
	l.ackedMu.Unlock()

	fmt.Printf("Leader received ack for tuple: %s\n", args.Tuple.Key)
	*reply = true
	return nil
}

func sendRPC(addr string, method string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("sendRPC: Failed to connect to %s: %v\n", addr, err)
		return err
	}
	defer client.Close()
	return client.Call(method, args, reply)
}

func (l *Leader) TaskFailed(args *rss.ReviveTaskArgs, reply *bool) error {
	l.mu.Lock()
	arg := args.Args

	// pick worker (round-robin)
	worker := l.workers[l.RoundRobinIndex]
	// advance round-robin for next time
	l.RoundRobinIndex = (l.RoundRobinIndex + 1) % len(l.workers)
	l.mu.Unlock()

	// Step 1: assign task on chosen worker (do network I/O WITHOUT holding leader lock)
	var assignReply int
	if err := sendRPC(worker, "Worker.AssignTask", arg, &assignReply); err != nil {
		return fmt.Errorf("assigning task %d to %s failed: %v", arg.TaskID, worker, err)
	}
	if assignReply == 0 {
		return fmt.Errorf("worker %s rejected revive for task %d", worker, arg.TaskID)
	}

	// Step 2: update leader's mapping for the revived task (hold lock)
	l.mu.Lock()
	l.taskMapping[arg.TaskID] = rss.TaskIPAndPID{IP: worker, PID: assignReply}
	// If revived task is in stage 0 (the first stage), do leader-only map fix

	if arg.Stage == 0 {
		l.mu.Unlock()
		*reply = true
		return nil
	}

	// Step 3: not first stage → we must notify upstream tasks in previous stage
	prevStage := arg.Stage - 1
	start := prevStage * tasksPerStage
	end := start + tasksPerStage // exclusive upper bound

	// collect upstream worker addresses while holding lock
	upstreamWorkers := make([]string, 0, tasksPerStage)
	for upstreamID := start; upstreamID < end; upstreamID++ {
		w, ok := l.taskMapping[upstreamID]
		if !ok {
			l.mu.Unlock()
			return fmt.Errorf("no worker mapping for upstream task %d", upstreamID)
		}
		upstreamWorkers = append(upstreamWorkers, w.IP)
	}
	l.mu.Unlock()

	// Step 4: notify upstream tasks (do RPCs without holding leader lock)
	// Build downstream info for the revived task
	downstreamInfo := rss.DownstreamInfo{TaskID: arg.TaskID, IP: worker}
	for i, upstreamWorker := range upstreamWorkers {
		upstreamTaskID := start + i
		var updateReply bool
		args := &rss.UpdateDownstreamArgs{
			TaskID:     upstreamTaskID,
			Downstream: downstreamInfo,
		}
		if err := sendRPC(upstreamWorker, "Worker.UpdateDownstream", args, &updateReply); err != nil {
			return fmt.Errorf("failed to update upstream task %d on %s: %v", upstreamTaskID, upstreamWorker, err)
		}
		if !updateReply {
			return fmt.Errorf("upstream worker %s declined update for task %d", upstreamWorker, upstreamTaskID)
		}
	}

	*reply = true
	return nil
}

// monitorSent periodically resends tuples that the leader has sent but not yet
// received an ack for from stage-1 tasks. It mirrors worker-side resend logic
// but uses leader's sentTuples and ackedTuples maps.
func (l *Leader) monitorSent() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// Copy unacked sent tuples while holding read locks
		unacked := []rss.Tuple{}
		keys := []string{}

		l.sentMu.RLock()
		for k, t := range l.sentTuples {
			unacked = append(unacked, t)
			keys = append(keys, k)
		}
		l.sentMu.RUnlock()

		for i, tuple := range unacked {
			key := keys[i]

			// If already acked, skip
			l.ackedMu.RLock()
			_, acked := l.ackedTuples[key]
			l.ackedMu.RUnlock()
			if acked {
				continue
			}

			// Get where to resend (which task/worker)
			l.sentMu.RLock()
			tid, ok := l.sentTask[key]
			l.sentMu.RUnlock()
			if !ok {
				continue
			}

			l.mu.Lock()
			wp, ok := l.taskMapping[tid]
			l.mu.Unlock()
			if !ok {
				// no mapping available (task might be down), skip for now
				continue
			}

			log.Printf("Leader: Tuple %s not acked, resending to task %d at %s", key, tid, wp.IP)
			var dummy bool
			args := &rss.AddTuplesArgs{
				TaskID:     tid,
				Tuples:     []rss.Tuple{tuple},
				SourceIP:   getLocalIP() + ":9300",
				SourceTask: -1,
			}
			if err := sendRPC(wp.IP, "Worker.AddTuples", args, &dummy); err != nil {
				log.Printf("Leader: failed to resend tuple %s to %s: %v", key, wp.IP, err)
			}
		}
	}
}

// --------------------------
// Helper methods
// --------------------------

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
		fmt.Printf("pingWorker: Failed to connect to %s: %v\n", addr, err)
		return false
	}
	defer client.Close()

	var ok bool
	callErr := client.Call("Worker.Heartbeat", &struct{}{}, &ok)
	return callErr == nil && ok
}
func (l *Leader) handleListTasks() {
    l.mu.Lock()
    currentWorkers := l.workers
    l.mu.Unlock()

    if len(currentWorkers) == 0 {
        fmt.Println("No workers connected.")
        return
    }

    fmt.Printf("%-10s %-20s %-10s %-15s %-30s\n", "TaskID", "VM IP", "PID", "Exe", "Log File")
    fmt.Println(strings.Repeat("-", 90))

    for _, workerAddr := range currentWorkers {
        args := rss.GetTaskStatusArgs{}
        var reply rss.GetTaskStatusReply

        // Call the worker
        err := sendRPC(workerAddr, "Worker.GetTaskStatus", &args, &reply)
        if err != nil {
            fmt.Printf("Failed to query worker %s: %v\n", workerAddr, err)
            continue
        }

        for _, report := range reply.Reports {
            fmt.Printf("%-10d %-20s %-10d %-15s %-30s\n", 
                report.TaskID, 
                workerAddr, 
                report.PID, 
                report.Exe, 
                report.LogFile,
            )
        }
    }
    fmt.Println(strings.Repeat("-", 90))
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
	inputRate, err = strconv.Atoi(parts[offset+4])
	if err != nil {
		return nil, fmt.Errorf("invalid INPUT_RATE: %v", err)
	}
	if autoscale {
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
	return stage*tasksPerStage + index
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
			worker := workers[tid%wcount] // round-robin
			taskToWorker[tid] = worker
			l.RoundRobinIndex = (l.RoundRobinIndex + 1) % wcount
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
				TaskID:            tid,
				Stage:             stage,
				Dest:              cmd.HydfsDest,
				Exe:               cmd.Ops[stage].Exe,
				Args:              cmd.Ops[stage].Args,
				Downstream:        downstream,
				Exactly_Once:      cmd.ExactlyOnce,
				Autoscale_Enabled: cmd.Autoscale,
				InputRate:         cmd.InputRate,
				LW:                cmd.LW,
				HW:                cmd.HW,
			}

			// Perform RPC
			var reply int
			if err := sendRPC(worker, "Worker.AssignTask", args, &reply); err != nil {
				return fmt.Errorf("assigning task %d to %s failed: %v", tid, worker, err)
			}

			// Save mapping
			l.taskMapping[tid] = rss.TaskIPAndPID{IP: worker, PID: reply}
		}
	}

	return nil
}

func (l *Leader) ReadFileAndSendTuples(filename string, nTasksStage1 int, inputRate int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	defer file.Close()

	leaderIP := getLocalIP() + ":9300" // Leader's IP with RPC port

	var ticker *time.Ticker
	if inputRate > 0 {
		// Calculate the interval between sends (1 second / rate)
		// e.g., if rate is 100, interval is 10ms
		interval := time.Duration(int64(time.Second) / int64(inputRate))
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		fmt.Printf("Source started with Input Rate: %d tuples/sec\n", inputRate)
	}

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		if inputRate > 0 {
			<-ticker.C
		}
		line := scanner.Text()
		key := fmt.Sprintf("%s:%d", filename, lineNum)
		tuple := rss.Tuple{
			Key:   key,
			Value: line,
		}

		// Hash key to pick stage 1 task
		taskIdx := int(rss.HashKey(tuple.Key)) % nTasksStage1
		taskID := taskID(0, taskIdx, nTasksStage1) // assuming stage 0 = first stage
		worker, ok := l.taskMapping[taskID]
		workerAddr := worker.IP
		if !ok {
			return fmt.Errorf("no worker assigned for task %d", taskID)
		}

		// Send the tuple via RPC - Leader provides its IP for ack-back
		args := rss.AddTuplesArgs{
			TaskID:     taskID,
			Tuples:     []rss.Tuple{tuple},
			SourceIP:   leaderIP, // Leader's IP so workers can ack back
			SourceTask: -1,       // -1 indicates this is from the leader/source
		}
		var reply bool
		if err := sendRPC(workerAddr, "Worker.AddTuples", &args, &reply); err != nil {
			fmt.Printf("failed to send tuple to worker %s task %d: %v\n", workerAddr, taskID, err)
			// optionally buffer for retry
		}

		// Record that leader sent this tuple so we can resend until acked
		l.sentMu.Lock()
		l.sentTuples[key] = tuple
		l.sentTask[key] = taskID
		l.sentMu.Unlock()

		lineNum++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", filename, err)
	}

	return nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Printf("Error getting addresses: %v\n", err)
		log.Fatal("Error getting addresses:", err)
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil && !ipnet.IP.IsLoopback() {
			return ipnet.IP.String()
		}
	}
	return ""
}

// --------------------------
// Main
// --------------------------

func main() {
	selfIP := getLocalIP()
	fmt.Println("Local IP:", selfIP)

	// Define the list of known VMs in the cluster
	vms := []string{
		"172.22.95.98:9000",
		"172.22.154.169:9000",
		"172.22.158.169:9000",
		"172.22.95.99:9000",
		"172.22.154.170:9000",
		"172.22.158.170:9000",
		"172.22.95.100:9000",
		"172.22.154.171:9000",
		"172.22.158.171:9000",
		"172.22.95.101:9000",
	}

	// Find the index of this node in the VM list for logging
	idx := -1
	for i, v := range vms {
		if v == (string(selfIP) + ":9000") {
			idx = i + 1
			break
		}
	}

	// Set up logging to both console and file
	filename := fmt.Sprintf("machine.%02d.log", idx)

	f, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	log.SetOutput(f)
	node = hydfs.Start()

	leader := &Leader{
		workers:         nil,                            // start empty
		taskMapping:     make(map[int]rss.TaskIPAndPID), // taskID → worker
		tupleBuffer:     make(map[int][]rss.Tuple),      // in-flight tuples
		ackedTuples:     make(map[string]rss.Tuple),     // tuples acked by stage 1
		sentTuples:      make(map[string]rss.Tuple),     // tuples sent by leader
		sentTask:        make(map[string]int),
		RoundRobinIndex: 0,
	}


	// Register leader RPC
	rpc.Register(leader)

	// Start listening for RPC connections
	ln, err := net.Listen("tcp", ":9300") // leader port
	if err != nil {
		fmt.Printf("Failed to listen on :9300: %v\n", err)
		log.Fatal(err)
	}
	fmt.Println("Leader RPC listening on port 9300")

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

	// CLI input loop
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Leader ready. Enter command:")

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Check for special CLI commands first
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}
		cmd_type := args[0]
		switch cmd_type {
		case "discover":
			leader.workers = discoverWorkers()
			fmt.Printf("Discovered %d workers: %+v\n", len(leader.workers), leader.workers)
			continue
		case "kill_task":
			parts := strings.Fields(line)
			if len(parts) < 2 {
				fmt.Println("Usage: kill_task <task_id>")
				continue
			}
			taskID, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Error parsing task ID:", err)
				continue
			}
			var reply bool
			worker, ok := leader.taskMapping[taskID]
			if !ok {
				fmt.Println("Task not found")
				continue
			}
			if err := sendRPC(worker.IP, "Worker.KillTask", &rss.KillTaskArgs{TaskID: taskID}, &reply); err != nil {
				fmt.Println("Error killing task:", err)
				continue
			}
			if !reply {
				fmt.Println("Failed to kill task")
				continue
			}
			fmt.Printf("Task %d killed successfully\n", taskID)
			continue
		case "list_tasks":
			leader.handleListTasks()
			continue
		}

		// Check if it's a hydfs command
		if hydfs.HydfsResponder(node, line) {
			continue
		}

		// Check if it's a failure detection command
		if node.Responder(line) {
			continue
		}

		// Parse RainStorm commands
		cmd, err := parseRainStormCommand(line)
		if err != nil {
			fmt.Println("Error parsing command:", err)
			continue
		}

		fmt.Printf("Parsed command: %+v\n", cmd)

		tasksPerStage = cmd.NtasksPerStage
		// Ensure workers are discovered before assigning tasks
		if len(leader.workers) == 0 {
			fmt.Println("No workers discovered. Please run `discover` first.")
			continue
		}

		hydfs.HandleCreate(node, "../emptyfile.txt", cmd.HydfsDest)
		leader.assignAllTasks(cmd)

		// Configure Exactly-Once behavior and start leader resend monitor if enabled
		leader.ExactlyOnce = cmd.ExactlyOnce
		if leader.ExactlyOnce {
			go leader.monitorSent()
		}

		go func() {
			err := leader.ReadFileAndSendTuples(cmd.HydfsSrc, cmd.NtasksPerStage, cmd.InputRate)
			if err != nil {
				fmt.Printf("Error in source stream: %v\n", err)
			}
		}()

		fmt.Println("Command processed. Enter next command:")
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}
