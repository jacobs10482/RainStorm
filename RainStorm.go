package main

import (
	"bufio"
	"fmt"
	rss "g51mp4/RainStormStructs"
	fd "g51mp4/failure_detection"
	hydfs "g51mp4/hydfs_system"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var node *fd.Node

// --------------------------
// Task structures
// --------------------------

type TupleWithSource struct {
	Tuple      rss.Tuple
	SourceIP   string // IP of the sender who sent this tuple
	SourceTask int    // TaskID of the sender
}

type TaskState struct {
	ID                  int
	Stage               int
	Exe                 string
	TaskArgs            rss.AssignTaskArgs
	HydfsDest           string
	LastStageOutputFile string
	Cmd                 *exec.Cmd
	Stdin               io.WriteCloser
	Stdout              io.ReadCloser
	InputQueue          chan TupleWithSource
	FailureChan         chan error
	ProcessedTuples     map[string]rss.Tuple // keyed by input TupleID, stores OUTPUT tuple
	AckedTuples         map[string]rss.Tuple // keyed by input TupleID
	OutputToInput       map[string]string    // maps output TupleID -> input TupleID (for ack lookup)
	ProcessedLogFile    string
	AckedLogFile        string
	mu1                 sync.RWMutex
	mu2                 sync.RWMutex
	mu3                 sync.RWMutex // protects OutputToInput
	Downstream          []rss.DownstreamInfo
	InputCount          int64
	SeqNum              int64 // atomic counter for generating output TupleIDs
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
	client, err := rpc.Dial("tcp", addr)
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

// parseTuple parses a log line in format "TupleID\tKey\tValue" or legacy "Key\tValue"
func parseTuple(line string) rss.Tuple {
	parts := strings.SplitN(line, "\t", 3)
	if len(parts) == 3 {
		// New format: TupleID\tKey\tValue
		return rss.Tuple{
			TupleID: parts[0],
			Key:     parts[1],
			Value:   parts[2],
		}
	} else if len(parts) == 2 {
		// Legacy format: Key\tValue (no TupleID)
		return rss.Tuple{
			TupleID: "",
			Key:     parts[0],
			Value:   parts[1],
		}
	}
	return rss.Tuple{
		TupleID: "",
		Key:     line,
		Value:   "",
	}
}

// Add this helper function to rainstorm.go

func loadStateFromHyDFS(logFilename string) (map[string]rss.Tuple, error) {
	stateMap := make(map[string]rss.Tuple)

	// 1. Try to get the file from HyDFS to a local temp file
	// We append .tmp to avoid conflict with the actual append-only log we will write to later
	localTempFile := "temp_recovery_" + logFilename

	// Assuming hydfs.HandleGet(node, remote, local) exists based on MP3 context
	// If the file doesn't exist in HyDFS (first time run), this might return an error.
	// We treat that as an empty state.
	hydfs.HandleGet(node, logFilename, localTempFile)
	defer os.Remove(localTempFile) // Clean up temp file

	// 2. Read the local file
	file, err := os.Open(localTempFile)
	if err != nil {
		return stateMap, nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		// Parse log format: TupleID\tKey\tValue
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) >= 3 {
			tuple := rss.Tuple{
				TupleID: parts[0],
				Key:     parts[1],
				Value:   parts[2],
			}
			stateMap[tuple.TupleID] = tuple
		}
	}

	fmt.Printf("Recovered %d items from %s\n", len(stateMap), logFilename)
	return stateMap, nil
}

func (w *Worker) AssignTask(args *rss.AssignTaskArgs, reply *int) error {
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

	// Define log filenames based on TaskID
	processedLog := fmt.Sprintf("processed_tuples_%d.log", args.TaskID)
	ackedLog := fmt.Sprintf("acked_tuples_%d.log", args.TaskID)

	// Attempt to recover state from HyDFS
	// Note: If files don't exist, these functions return empty maps (fresh start)
	recoveredProcessed, _ := loadStateFromHyDFS(processedLog)
	recoveredAcked, _ := loadStateFromHyDFS(ackedLog)

	ts := &TaskState{
		ID:                  args.TaskID,
		Stage:               args.Stage,
		Cmd:                 cmd,
		Exe:                 args.Exe,
		HydfsDest:           args.Dest,
		Stdin:               stdin,
		Stdout:              stdout,
		Downstream:          args.Downstream,
		LastStageOutputFile: fmt.Sprintf("last_stage_output_%d.txt", args.TaskID),
		ProcessedTuples:     recoveredProcessed,
		AckedTuples:         recoveredAcked,
		OutputToInput:       make(map[string]string),         // maps output TupleID -> input TupleID
		InputQueue:          make(chan TupleWithSource, 100), // Buffered channel
		ProcessedLogFile:    processedLog,
		AckedLogFile:        ackedLog,
		FailureChan:         make(chan error, 1),
		TaskArgs:            *args,
	}
	if len(recoveredProcessed) == 0 {
		hydfs.HandleCreate(node, "../emptyfile.txt", ts.ProcessedLogFile)
		hydfs.HandleCreate(node, "../emptyfile.txt", ts.AckedLogFile)
	}

	tasks[args.TaskID] = ts
	fmt.Printf("Assigned task: %s, Args: %s\n", args.Exe, args.Args)

	// Start goroutine to process tuples
	go processTuples(ts)
	if args.Exactly_Once {
		go monitorAcks(ts)
		go monitorTaskFailure(ts)
	} else {
		// If Exactly_Once is disabled, still start a lightweight failure watcher
		// to remove task state when the process exits, but do not perform ack-based resends.
		go func() {
			_ = ts.Cmd.Wait()
			// notify via FailureChan to stop processing goroutines
			// broadcast close to all
			close(ts.FailureChan)
			// cleanup
			tasksMu.Lock()
			delete(tasks, ts.ID)
			tasksMu.Unlock()
		}()
	}

	*reply = cmd.Process.Pid

	return nil
}
func (w *Worker) GetTaskStatus(args *rss.GetTaskStatusArgs, reply *rss.GetTaskStatusReply) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	var reports []rss.TaskReport

	for _, ts := range tasks {
		pid := -1
		if ts.Cmd != nil && ts.Cmd.Process != nil {
			pid = ts.Cmd.Process.Pid
		}

		reports = append(reports, rss.TaskReport{
			TaskID:  ts.ID,
			PID:     pid,
			Exe:     ts.Exe,
			LogFile: ts.ProcessedLogFile,
		})
	}

	reply.Reports = reports
	return nil
}
func monitorTaskFailure(ts *TaskState) {
	// Wait for the command to finish
	err := ts.Cmd.Wait()

	// Close the channel to broadcast failure to ALL listening goroutines (processTuples, monitorAcks)
	// Sending a single value only wakes one of them, leaving the other running on stale state (zombie).
	close(ts.FailureChan)

	if err != nil {
		log.Printf("task %d process exited with error: %v\n", ts.ID, err)
	} else {
		log.Printf("task %d process exited normally\n", ts.ID)
	}

	// Remove task from local map (if not already removed by KillTask)
	tasksMu.Lock()
	delete(tasks, ts.ID)
	tasksMu.Unlock()

	var reply bool
	args := &rss.ReviveTaskArgs{Args: &ts.TaskArgs}
	// call leader's TaskFailed RPC using the stored LeaderIP
	leaderIP := ts.TaskArgs.LeaderIP
	if leaderIP == "" {
		leaderIP = "172.22.95.98:9300" // fallback
	}
	if rpcErr := sendRPC(leaderIP, "Leader.TaskFailed", args, &reply); rpcErr != nil {
		log.Printf("Failed to notify leader about task %d: %v\n", ts.ID, rpcErr)
	} else {
		log.Printf("Notified leader that task %d failed\n", ts.ID)
	}

}

func monitorAcks(ts *TaskState) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case err := <-ts.FailureChan:
			// Something was sent: exit the goroutine
			log.Printf("Task %d failed, exiting goroutine: %v", ts.ID, err)
			return
		default:
			// Channel empty: continue normally
		}

		// Make a copy of unacked tuples to avoid holding lock during RPC
		// Store both inputTupleID and outputTuple for resend
		type unackedEntry struct {
			inputTupleID string
			outputTuple  rss.Tuple
		}
		unackedTuples := []unackedEntry{}

		ts.mu1.RLock()
		ts.mu2.RLock()
		for inputTupleID, outputTuple := range ts.ProcessedTuples {
			if _, acked := ts.AckedTuples[inputTupleID]; !acked {
				unackedTuples = append(unackedTuples, unackedEntry{inputTupleID, outputTuple})
			}
		}
		ts.mu2.RUnlock()
		ts.mu1.RUnlock()

		// Now resend without holding locks
		for _, entry := range unackedTuples {
			// Double-check it's still unacked (might have been acked during copy)
			ts.mu2.RLock()
			_, acked := ts.AckedTuples[entry.inputTupleID] // Use inputTupleID, not output's TupleID
			ts.mu2.RUnlock()

			if acked {
				continue // Got acked while we were copying, skip it
			}

			log.Printf("Task %d: Tuple %s (TupleID: %s) not acked, resending output", ts.ID, entry.outputTuple.Key, entry.outputTuple.TupleID)

			if len(ts.Downstream) == 0 {
				continue // Final stage, nothing to resend to
			}

			idx := int(HashKey(entry.outputTuple.Key)) % len(ts.Downstream)
			target := ts.Downstream[idx]

			var dummyReply bool
			err := sendRPC(target.IP, "Worker.AddTuples",
				&rss.AddTuplesArgs{
					TaskID:     target.TaskID,
					Tuples:     []rss.Tuple{entry.outputTuple},
					SourceIP:   getLocalIP() + ":9300",
					SourceTask: ts.ID,
				},
				&dummyReply,
			)

			if err != nil {
				log.Printf("Task %d: Failed to resend tuple %s: %v", ts.ID, entry.outputTuple.TupleID, err)
				// Could notify leader of downstream failure here
			}
		}
	}
}

// working state
func (w *Worker) UpdateDownstream(args *rss.UpdateDownstreamArgs, reply *bool) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	ts, ok := tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	idx := args.DownstreamIndex
	// Expand slice if needed
	for len(ts.Downstream) <= idx {
		ts.Downstream = append(ts.Downstream, rss.DownstreamInfo{})
	}
	ts.Downstream[idx] = args.Downstream
	*reply = true
	return nil
}

// AddDownstream appends a new downstream task to this task's downstream list
func (w *Worker) AddDownstream(args *rss.AddDownstreamArgs, reply *bool) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	ts, ok := tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	ts.Downstream = append(ts.Downstream, args.Downstream)
	*reply = true
	return nil
}

// RemoveDownstream removes a downstream task by TaskID from this task's downstream list
func (w *Worker) RemoveDownstream(args *rss.RemoveDownstreamArgs, reply *bool) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	ts, ok := tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	// Find and remove the downstream entry with matching TaskID
	newDownstream := make([]rss.DownstreamInfo, 0, len(ts.Downstream))
	for _, ds := range ts.Downstream {
		if ds.TaskID != args.DownstreamTaskID {
			newDownstream = append(newDownstream, ds)
		}
	}
	ts.Downstream = newDownstream
	*reply = true
	return nil
}

// UpdateDownstreamByTaskID finds a downstream entry by TaskID and updates it (for revival)
func (w *Worker) UpdateDownstreamByTaskID(args *rss.UpdateDownstreamByTaskIDArgs, reply *bool) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	ts, ok := tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	// Find and update the downstream entry with matching TaskID
	found := false
	for i, ds := range ts.Downstream {
		if ds.TaskID == args.DownstreamTaskID {
			ts.Downstream[i] = args.NewDownstream
			found = true
			break
		}
	}

	if !found {
		// Task wasn't in downstream list, add it
		ts.Downstream = append(ts.Downstream, args.NewDownstream)
	}

	*reply = true
	return nil
}

// Main processing goroutine - reads from queue, processes, acks, and forwards
func processTuples(ts *TaskState) {
	// Create a scanner to read from stdout
	scanner := bufio.NewScanner(ts.Stdout)

	for tupleWithSource := range ts.InputQueue {
		fmt.Printf("Task %d processing tuple: %s (TupleID: %s)\n", ts.ID, tupleWithSource.Tuple.Key, tupleWithSource.Tuple.TupleID)
		select {
		case err := <-ts.FailureChan:
			// Something was sent: exit the goroutine
			log.Printf("Task %d failed, exiting goroutine: %v", ts.ID, err)
			return
		default:
			// Channel empty: continue normally
		}

		tuple := tupleWithSource.Tuple

		// Check for duplicates using TupleID (idempotency)
		ts.mu1.RLock()
		if _, exists := ts.ProcessedTuples[tuple.TupleID]; exists {
			ts.mu1.RUnlock()
			// Already processed, send ack anyway
			sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
			continue
		}
		ts.mu1.RUnlock()

		fmt.Printf("Input Tuple: %s, Value: %s\n", tuple.Key, tuple.Value)
		// Write tuple to operator's stdin
		_, err := fmt.Fprintf(ts.Stdin, "%s\t%s\n", tuple.Key, tuple.Value)
		if err != nil {
			log.Printf("Error writing to stdin for task %d: %v", ts.ID, err)
			continue
		}

		// Read output from operator's stdout (format: Key\tValue)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading from stdout for task %d: %v", ts.ID, err)
			}
			break
		}

		outputLine := scanner.Text()
		// Operator outputs Key\tValue, we need to parse and generate TupleID
		parts := strings.SplitN(outputLine, "\t", 2)
		var outputKey, outputValue string
		if len(parts) == 2 {
			outputKey = parts[0]
			outputValue = parts[1]
		} else {
			outputKey = outputLine
			outputValue = ""
		}

		// Keep the same TupleID for the output tuple (propagate through stages)
		// This way when downstream acks, it sends back tupleID that we can directly use
		outputTuple := rss.Tuple{
			TupleID: tuple.TupleID, // Same TupleID propagated from input
			Key:     outputKey,
			Value:   outputValue,
		}

		// Check for __DROP__ BEFORE storing in ProcessedTuples
		// This prevents monitorAcks from resending dropped tuples to downstream
		if outputTuple.Value == "__DROP__" {
			if ts.TaskArgs.Exactly_Once {
				// Mark as processed
				ts.mu1.Lock()
				ts.ProcessedTuples[tuple.TupleID] = outputTuple
				ts.mu1.Unlock()

				// Mark as acked immediately so monitorAcks doesn't resend
				ts.mu2.Lock()
				ts.AckedTuples[tuple.TupleID] = outputTuple
				ts.mu2.Unlock()

				// Just ack back to the source (filtered out), don't forward
				sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
			}
			continue
		}

		// Store and log for recovery (only if exactly_once is enabled)
		if ts.TaskArgs.Exactly_Once {
			ts.mu1.Lock()
			ts.ProcessedTuples[tuple.TupleID] = outputTuple
			ts.mu1.Unlock()

			// Write to log: TupleID\tKey\tValue (for recovery)
			line := fmt.Sprintf("%s\t%s\t%s\n", tuple.TupleID, outputTuple.Key, outputTuple.Value)
			hydfs.HandleAppendString(node, line, ts.ProcessedLogFile)
		}

		// Handle final stage vs intermediate stage
		if len(ts.Downstream) == 0 {
			// Final stage - write to HyDFS

			fmt.Printf("%s\t%s\n", outputTuple.Key, outputTuple.Value)

			lineOutput := fmt.Sprintf("%s\t%s\n", outputTuple.Key, outputTuple.Value)
			os.WriteFile(ts.LastStageOutputFile, []byte(lineOutput), 0644)
			hydfs.HandleAppend(node, ts.LastStageOutputFile, ts.HydfsDest)
			os.WriteFile(ts.LastStageOutputFile, []byte{}, 0644)

			// Send ack back to source (only if exactly_once is enabled)
			if ts.TaskArgs.Exactly_Once {
				sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
			}
		} else {
			// Intermediate stage - forward to downstream
			idx := int(HashKey(outputTuple.Key)) % len(ts.Downstream)
			target := ts.Downstream[idx]

			// Forward with retry logic
			// For exactly_once: retry forever (with backoff) to ensure delivery
			// For non-exactly_once: limited retries then give up
			maxRetries := 10
			if ts.TaskArgs.Exactly_Once {
				maxRetries = -1 // infinite retries
			}

			for attempt := 0; maxRetries < 0 || attempt < maxRetries; attempt++ {
				var dummyReply bool
				err := sendRPC(target.IP, "Worker.AddTuples",
					&rss.AddTuplesArgs{
						TaskID:     target.TaskID,
						Tuples:     []rss.Tuple{outputTuple},
						SourceIP:   getLocalIP() + ":9300",
						SourceTask: ts.ID,
					},
					&dummyReply,
				)

				if err == nil {
					// Successfully forwarded, send ack (only if exactly_once is enabled)
					if ts.TaskArgs.Exactly_Once {
						sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
					}
					break
				}

				log.Printf("Failed to forward tuple to %s (attempt %d), retrying...", target.IP, attempt+1)
				time.Sleep(100 * time.Millisecond) // backoff
			}
		}
	}
}

func sendAck(sourceIP string, sourceTaskID int, tuple rss.Tuple) {
	if sourceIP == "" {
		// No upstream source
		return
	}

	var ackReply bool
	var method string

	// If sourceTaskID is -1, it's the leader, otherwise it's a worker
	if sourceTaskID == -1 {
		method = "Leader.AckTuple"
	} else {
		method = "Worker.AckTuple"
	}

	err := sendRPC(sourceIP, method,
		&rss.TupleOutputArgs{
			TaskID: sourceTaskID,
			Tuple:  tuple,
		},
		&ackReply,
	)

	if err != nil {
		log.Printf("Failed to send ack to %s for task %d: %v", sourceIP, sourceTaskID, err)
	}
}

func (w *Worker) AckTuple(args *rss.TupleOutputArgs, reply *bool) error {
	tasksMu.Lock()
	ts, ok := tasks[args.TaskID]
	tasksMu.Unlock()

	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	// The ack contains the tuple with its TupleID - use it directly
	ts.mu2.Lock()
	ts.AckedTuples[args.Tuple.TupleID] = args.Tuple
	ts.mu2.Unlock()

	// Write to log for recovery
	line := fmt.Sprintf("%s\t%s\t%s\n", args.Tuple.TupleID, args.Tuple.Key, args.Tuple.Value)
	hydfs.HandleAppendString(node, line, ts.AckedLogFile)

	fmt.Printf("Task %d received ack for TupleID: %s\n", args.TaskID, args.Tuple.TupleID)
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

	// Add tuples to the processing queue
	for _, t := range args.Tuples {
		// increment per-task input counter for autoscale metrics
		atomic.AddInt64(&ts.InputCount, 1)
		ts.InputQueue <- TupleWithSource{
			Tuple:      t,
			SourceIP:   args.SourceIP,
			SourceTask: args.SourceTask,
		}
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

	// Close the input queue to stop the processing goroutine
	close(ts.InputQueue)

	ts.Cmd.Process.Kill()
	ts.Stdin.Close()
	ts.Stdout.Close()

	*reply = true
	return nil
}

// GetQueueLen returns the current length of the input queue for the given task.
func (w *Worker) GetQueueLen(args *rss.GetQueueLenArgs, reply *rss.QueueLenReply) error {
	tasksMu.Lock()
	ts, ok := tasks[args.TaskID]
	tasksMu.Unlock()

	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	// len(channel) gives the number of queued items
	reply.Length = len(ts.InputQueue)
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

// collectAndReportMetrics runs on each worker and reports per-task input rates
// to the leader every 2 seconds when tasks have autoscale enabled.
func collectAndReportMetrics() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Build metrics per leader address
		leaderMap := make(map[string][]rss.TaskMetric)

		tasksMu.Lock()
		for _, ts := range tasks {
			if !ts.TaskArgs.Autoscale_Enabled {
				continue
			}
			// swap the input count to get number of inputs in last interval
			cnt := atomic.SwapInt64(&ts.InputCount, 0)
			rate := float64(cnt) / 2.0
			leaderAddr := ts.TaskArgs.LeaderIP
			if leaderAddr == "" {
				// skip if leader not provided
				continue
			}
			leaderMap[leaderAddr] = append(leaderMap[leaderAddr], rss.TaskMetric{
				TaskID: ts.ID,
				Stage:  ts.Stage,
				Rate:   rate,
				Worker: getLocalIP() + ":9300",
			})
		}
		tasksMu.Unlock()

		// Send metrics grouped by leader address
		for leaderAddr, metrics := range leaderMap {
			args := &rss.MetricsArgs{Metrics: metrics}
			var reply bool
			if err := sendRPC(leaderAddr, "Leader.ReportMetrics", args, &reply); err != nil {
				log.Printf("Failed to send metrics to leader %s: %v", leaderAddr, err)
			}
		}
	}
}

// --------------------------
// Main RPC listener
// --------------------------

func main() {
	selfIP := getLocalIP()
	fmt.Println("Local IP:", selfIP)

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

	idx := -1
	for i, v := range vms {
		if v == (string(selfIP) + ":9000") {
			idx = i + 1
			break
		}
	}

	filename := fmt.Sprintf("machine.%02d.log", idx)
	f, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	node = hydfs.Start()
	ip := getLocalIP()
	port := "9300"

	worker := &Worker{}
	rpc.Register(worker)

	ln, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Worker RPC listening on port %s\n", port)

	// start metrics reporter for autoscale
	go collectAndReportMetrics()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
