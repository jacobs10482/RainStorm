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
	"sync/atomic"
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
	TaskID   int
	Stage    int
	Exe      string
	Args     []string
	LeaderIP string
}

// --------------------------
// Leader state
// --------------------------

type Leader struct {
	mu              sync.Mutex
	workers         []string                 // set of alive workers
	taskMapping     map[int]rss.TaskIPAndPID // taskID → worker addr
	tupleBuffer     map[int][]rss.Tuple
	ackedTuples     map[string]rss.Tuple // Track acked tuples from stage 1, keyed by TupleID
	ackedMu         sync.RWMutex
	sentTuples      map[string]rss.Tuple // Track tuples sent by leader, keyed by TupleID
	sentTask        map[string]int       // mapping TupleID -> taskID it was sent to
	sentMu          sync.RWMutex
	ExactlyOnce     bool
	Autoscale       bool
	Nstages         int
	Ops             []StageOp
	StageHW         int
	StageLW         int
	activeTasks     map[int][]int   // stage -> list of taskIDs
	metricsPerTask  map[int]float64 // taskID -> last reported rate
	metricsMu       sync.Mutex
	RoundRobinIndex int
	nextTaskID      int   // counter for generating unique task IDs
	leaderSeqNum    int64 // atomic counter for generating leader TupleIDs
}

// --------------------------
// RPC Methods for Leader
// --------------------------

func (l *Leader) AckTuple(args *rss.TupleOutputArgs, reply *bool) error {
	// Leader receives acks from first stage tasks, keyed by TupleID
	l.ackedMu.Lock()
	l.ackedTuples[args.Tuple.TupleID] = args.Tuple
	l.ackedMu.Unlock()

	fmt.Printf("Leader received ack for tuple: %s (TupleID: %s)\n", args.Tuple.Key, args.Tuple.TupleID)
	*reply = true
	return nil
}

// ReportMetrics receives per-task input rate metrics from workers.
func (l *Leader) ReportMetrics(args *rss.MetricsArgs, reply *bool) error {
	l.metricsMu.Lock()
	defer l.metricsMu.Unlock()

	for _, m := range args.Metrics {
		l.metricsPerTask[m.TaskID] = m.Rate
	}

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

// monitorMetrics periodically (every 2s) scans the stored per-task metrics,
// computes average rate per stage and scales up/down based on HW/LW.
func (l *Leader) monitorMetrics() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !l.Autoscale {
			return
		}

		l.metricsMu.Lock()
		// copy metrics map to avoid holding lock during computation
		mcopy := make(map[int]float64, len(l.metricsPerTask))
		for k, v := range l.metricsPerTask {
			mcopy[k] = v
		}
		l.metricsMu.Unlock()

		// For each stage, compute average over active tasks
		for stage := 0; stage < l.Nstages; stage++ {
			l.mu.Lock()
			ids := append([]int(nil), l.activeTasks[stage]...)
			l.mu.Unlock()

			if len(ids) == 0 {
				continue
			}

			sum := 0.0
			for _, tid := range ids {
				if r, ok := mcopy[tid]; ok {
					sum += r
				}
			}
			avg := sum / float64(len(ids))

			log.Printf("monitorMetrics: stage %d avg_rate=%.2f count=%d (HW=%d LW=%d)", stage, avg, len(ids), l.StageHW, l.StageLW)

			// Scale up: average rate > HW
			if l.StageHW > 0 && avg > float64(l.StageHW) {
				log.Printf("monitorMetrics: stage %d adding task (avg %.2f > HW %d)", stage, avg, l.StageHW)
				if err := l.addTaskToStage(stage); err != nil {
					log.Printf("monitorMetrics: failed to add task to stage %d: %v", stage, err)
				}
			} else if l.StageLW > 0 && avg < float64(l.StageLW) && len(ids) > 1 {
				// Scale down: average rate < LW, but keep at least 1 task
				log.Printf("monitorMetrics: stage %d removing task (avg %.2f < LW %d)", stage, avg, l.StageLW)
				if err := l.removeTaskFromStage(stage); err != nil {
					log.Printf("monitorMetrics: failed to remove task from stage %d: %v", stage, err)
				}
			}
		}
	}
}

// addTaskToStage creates a new task for the given stage, assigns it to a worker,
// and updates upstream tasks with the new downstream.
func (l *Leader) addTaskToStage(stage int) error {
	l.mu.Lock()
	if len(l.workers) == 0 {
		l.mu.Unlock()
		return fmt.Errorf("no workers available")
	}

	// Generate new task ID
	newTaskID := l.nextTaskID
	l.nextTaskID++

	// Pick worker via round-robin
	worker := l.workers[l.RoundRobinIndex%len(l.workers)]
	l.RoundRobinIndex++

	// Build downstream list (if not final stage)
	var downstream []rss.DownstreamInfo
	if stage < l.Nstages-1 {
		nextStageIDs := l.activeTasks[stage+1]
		for _, dtid := range nextStageIDs {
			if wp, ok := l.taskMapping[dtid]; ok {
				downstream = append(downstream, rss.DownstreamInfo{IP: wp.IP, TaskID: dtid})
			}
		}
	}

	// Get stage operator info
	op := l.Ops[stage]
	l.mu.Unlock()

	// Build AssignTask args
	args := &rss.AssignTaskArgs{
		TaskID:            newTaskID,
		Stage:             stage,
		Dest:              "", // will be set from existing tasks or command
		Exe:               op.Exe,
		Args:              op.Args,
		LeaderIP:          getLocalIP() + ":9300",
		Downstream:        downstream,
		Exactly_Once:      l.ExactlyOnce,
		Autoscale_Enabled: l.Autoscale,
		LW:                l.StageLW,
		HW:                l.StageHW,
	}

	// Assign task on worker
	var reply int
	if err := sendRPC(worker, "Worker.AssignTask", args, &reply); err != nil {
		return fmt.Errorf("failed to assign task %d to %s: %v", newTaskID, worker, err)
	}

	// Update leader state
	l.mu.Lock()
	l.taskMapping[newTaskID] = rss.TaskIPAndPID{IP: worker, PID: reply}
	l.activeTasks[stage] = append(l.activeTasks[stage], newTaskID)
	l.mu.Unlock()

	// Update upstream tasks with new downstream
	if stage > 0 {
		if err := l.updateUpstreamDownstreams(stage); err != nil {
			log.Printf("addTaskToStage: failed to update upstream for stage %d: %v", stage, err)
		}
	}

	log.Printf("addTaskToStage: added task %d to stage %d on worker %s", newTaskID, stage, worker)
	return nil
}

// removeTaskFromStage removes one task from the given stage, kills it,
// and updates upstream tasks with the new downstream list.
func (l *Leader) removeTaskFromStage(stage int) error {
	l.mu.Lock()
	ids := l.activeTasks[stage]
	if len(ids) <= 1 {
		// Do not remove the last task in a stage. Return nil so callers
		// treat this as a no-op rather than a fatal error.
		log.Printf("removeTaskFromStage: skipping removal for stage %d because only %d task(s) remain", stage, len(ids))
		l.mu.Unlock()
		return nil
	}

	// Remove last task in the list
	taskToRemove := ids[len(ids)-1]
	l.activeTasks[stage] = ids[:len(ids)-1]

	wp, ok := l.taskMapping[taskToRemove]
	if !ok {
		l.mu.Unlock()
		return fmt.Errorf("task %d not found in taskMapping", taskToRemove)
	}
	delete(l.taskMapping, taskToRemove)
	l.mu.Unlock()

	// Remove from metrics
	l.metricsMu.Lock()
	delete(l.metricsPerTask, taskToRemove)
	l.metricsMu.Unlock()

	// Update upstream tasks with new downstream list (without the removed task)
	if stage > 0 {
		if err := l.updateUpstreamDownstreams(stage); err != nil {
			log.Printf("removeTaskFromStage: failed to update upstream for stage %d: %v", stage, err)
		}
	}

	// Wait for the worker to drain its input queue before killing the task.
	// Poll the worker via Worker.GetQueueLen RPC. If the RPC fails or a
	// timeout elapses, proceed to kill to avoid indefinite waits.
	drained := false
	timeout := time.After(30 * time.Second)
	pollTicker := time.NewTicker(500 * time.Millisecond)
	for !drained {
		select {
		case <-timeout:
			log.Printf("removeTaskFromStage: timeout waiting for task %d to drain; proceeding to kill", taskToRemove)
			drained = true
		case <-pollTicker.C:
			var qreply rss.QueueLenReply
			if err := sendRPC(wp.IP, "Worker.GetQueueLen", &rss.GetQueueLenArgs{TaskID: taskToRemove}, &qreply); err != nil {
				log.Printf("removeTaskFromStage: failed to query queue length for task %d at %s: %v; proceeding to kill", taskToRemove, wp.IP, err)
				drained = true
				break
			}
			if qreply.Length == 0 {
				drained = true
				break
			}
			log.Printf("removeTaskFromStage: waiting for task %d to drain; queue length=%d", taskToRemove, qreply.Length)
		}
	}
	pollTicker.Stop()

	// Kill the task on the worker
	var reply bool
	if err := sendRPC(wp.IP, "Worker.KillTask", &rss.KillTaskArgs{TaskID: taskToRemove}, &reply); err != nil {
		log.Printf("removeTaskFromStage: failed to kill task %d: %v", taskToRemove, err)
	}

	log.Printf("removeTaskFromStage: removed task %d from stage %d", taskToRemove, stage)
	return nil
}

// updateUpstreamDownstreams rebuilds the downstream list for all tasks in stage-1
// based on the current activeTasks[stage] and sends updates to workers.
func (l *Leader) updateUpstreamDownstreams(stage int) error {
	if stage == 0 {
		return nil // stage 0 has no upstream tasks (only leader)
	}

	l.mu.Lock()
	upstreamIDs := append([]int(nil), l.activeTasks[stage-1]...)
	downstreamIDs := append([]int(nil), l.activeTasks[stage]...)

	// Build new downstream list
	var newDownstream []rss.DownstreamInfo
	for _, dtid := range downstreamIDs {
		if wp, ok := l.taskMapping[dtid]; ok {
			newDownstream = append(newDownstream, rss.DownstreamInfo{IP: wp.IP, TaskID: dtid})
		}
	}

	// Collect upstream worker info
	upstreamWorkers := make(map[int]string) // taskID -> worker IP
	for _, tid := range upstreamIDs {
		if wp, ok := l.taskMapping[tid]; ok {
			upstreamWorkers[tid] = wp.IP
		}
	}
	l.mu.Unlock()

	// Send update to each upstream task
	for tid, workerIP := range upstreamWorkers {
		for i, ds := range newDownstream {
			args := &rss.UpdateDownstreamArgs{
				TaskID:          tid,
				DownstreamIndex: i,
				Downstream:      ds,
			}
			var reply bool
			if err := sendRPC(workerIP, "Worker.UpdateDownstream", args, &reply); err != nil {
				log.Printf("updateUpstreamDownstreams: failed to update task %d downstream[%d]: %v", tid, i, err)
			}
		}
	}

	return nil
}

func (l *Leader) TaskFailed(args *rss.ReviveTaskArgs, reply *bool) error {
	l.mu.Lock()
	arg := args.Args

	// Check if we have workers available
	if len(l.workers) == 0 {
		l.mu.Unlock()
		return fmt.Errorf("no workers available to revive task %d", arg.TaskID)
	}

	// pick worker (round-robin)
	worker := l.workers[l.RoundRobinIndex%len(l.workers)]
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
	upstreamIDs := append([]int(nil), l.activeTasks[prevStage]...)

	// Find which index in downstream the revived task should occupy
	currentStageIDs := l.activeTasks[arg.Stage]
	downstreamIndex := -1
	for i, tid := range currentStageIDs {
		if tid == arg.TaskID {
			downstreamIndex = i
			break
		}
	}
	if downstreamIndex == -1 {
		// Task not in activeTasks yet for this stage, add it
		l.activeTasks[arg.Stage] = append(l.activeTasks[arg.Stage], arg.TaskID)
		downstreamIndex = len(l.activeTasks[arg.Stage]) - 1
	}

	// collect upstream worker addresses while holding lock
	upstreamWorkers := make(map[int]string) // taskID -> worker IP
	for _, upstreamID := range upstreamIDs {
		w, ok := l.taskMapping[upstreamID]
		if !ok {
			l.mu.Unlock()
			return fmt.Errorf("no worker mapping for upstream task %d", upstreamID)
		}
		upstreamWorkers[upstreamID] = w.IP
	}
	l.mu.Unlock()

	// Step 4: notify upstream tasks (do RPCs without holding leader lock)
	// Build downstream info for the revived task
	downstreamInfo := rss.DownstreamInfo{TaskID: arg.TaskID, IP: worker}
	for upstreamTaskID, upstreamWorkerIP := range upstreamWorkers {
		var updateReply bool
		args2 := &rss.UpdateDownstreamArgs{
			TaskID:          upstreamTaskID,
			DownstreamIndex: downstreamIndex,
			Downstream:      downstreamInfo,
		}
		if err := sendRPC(upstreamWorkerIP, "Worker.UpdateDownstream", args2, &updateReply); err != nil {
			return fmt.Errorf("failed to update upstream task %d on %s: %v", upstreamTaskID, upstreamWorkerIP, err)
		}
		if !updateReply {
			return fmt.Errorf("upstream worker %s declined update for task %d", upstreamWorkerIP, upstreamTaskID)
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

			log.Printf("Leader: TupleID %s not acked, resending to task %d at %s", tuple.TupleID, tid, wp.IP)
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

func splitArgs(input string) ([]string, error) {
	var args []string
	var current strings.Builder
	inQuote := false

	for _, r := range input {
		switch r {
		case '"', '“', '”':
			if inQuote {
				// Closing quote, finish current arg
				args = append(args, current.String())
				current.Reset()
				inQuote = false
			} else {
				// Opening quote
				inQuote = true
			}
		case ' ', '\t':
			if inQuote {
				current.WriteRune(r) // inside quotes, keep spaces
			} else {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	// If still inQuote at the end, just close it (for mismatched quotes)
	if inQuote {
		// optionally, you could log a warning
	}

	return args, nil
}

func parseRainStormCommand(line string) (*RainStormCommand, error) {
	parts, err := splitArgs(line)
	if err != nil {
		return nil, fmt.Errorf("failed to parse command: %v", err)
	}

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

	fmt.Printf("Stages: %d\n", nStages)
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
		fmt.Printf("Args: %s\n", arg)
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
				LeaderIP:          getLocalIP() + ":9300",
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

			// Track active tasks per stage
			l.activeTasks[stage] = append(l.activeTasks[stage], tid)

			// Update nextTaskID to be beyond all assigned IDs
			if tid >= l.nextTaskID {
				l.nextTaskID = tid + 1
			}
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

		// Generate unique TupleID for leader-generated tuples
		seqNum := atomic.AddInt64(&l.leaderSeqNum, 1)
		tupleID := fmt.Sprintf("leader:%d", seqNum)

		tuple := rss.Tuple{
			TupleID: tupleID,
			Key:     key,
			Value:   line,
		}

		// Choose stage-1 task from the current live list instead of relying
		// on a static numbering. This ensures autoscale (add/remove tasks)
		// doesn't cause the leader to send keys to the wrong task.
		l.mu.Lock()
		stage1IDs := append([]int(nil), l.activeTasks[0]...)
		l.mu.Unlock()

		if len(stage1IDs) == 0 {
			return fmt.Errorf("no active tasks in stage 0 to send tuples to")
		}

		idx := int(rss.HashKey(tuple.Key)) % len(stage1IDs)
		taskID := stage1IDs[idx]

		l.mu.Lock()
		wp, ok := l.taskMapping[taskID]
		l.mu.Unlock()
		if !ok {
			return fmt.Errorf("no worker assigned for task %d", taskID)
		}
		workerAddr := wp.IP

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

		// Record that leader sent this tuple so we can resend until acked (keyed by TupleID)
		l.sentMu.Lock()
		l.sentTuples[tupleID] = tuple
		l.sentTask[tupleID] = taskID
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
		activeTasks:     make(map[int][]int),
		metricsPerTask:  make(map[int]float64),
		nextTaskID:      0,
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

		// Configure Autoscale behavior and store parameters
		leader.Autoscale = cmd.Autoscale
		leader.Nstages = cmd.Nstages
		leader.StageHW = cmd.HW
		leader.StageLW = cmd.LW
		leader.Ops = cmd.Ops
		if leader.Autoscale {
			go leader.monitorMetrics()
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
