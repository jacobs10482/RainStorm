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
	"time"
)

var node *fd.Node

// --------------------------
// Task structures
// --------------------------

type TupleWithSource struct {
	Tuple      rss.Tuple
	SourceIP   string  // IP of the sender who sent this tuple
	SourceTask int     // TaskID of the sender
}

type TaskState struct {
	ID                  int
	Stage               int
	HydfsDest           string
	LastStageOutputFile string
	Cmd                 *exec.Cmd
	Stdin               io.WriteCloser
	Stdout              io.ReadCloser
	InputQueue          chan TupleWithSource
	ProcessedTuples     map[string]rss.Tuple
	AckedTuples         map[string]rss.Tuple
	ProcessedLogFile    string
    AckedLogFile        string
	mu1                 sync.RWMutex
	mu2                 sync.RWMutex
	Downstream          []rss.DownstreamInfo
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

func parseTuple(line string) rss.Tuple {
	parts := strings.SplitN(line, "\t", 2)
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
		ID:                  args.TaskID,
		Stage:               args.Stage,
		Cmd:                 cmd,
		HydfsDest:           args.Dest,
		Stdin:               stdin,
		Stdout:              stdout,
		Downstream:          args.Downstream,
		LastStageOutputFile: fmt.Sprintf("last_stage_output_%d.txt", args.TaskID),
		ProcessedTuples:     make(map[string]rss.Tuple),
		AckedTuples:         make(map[string]rss.Tuple),
		InputQueue:          make(chan TupleWithSource, 100), // Buffered channel
		ProcessedLogFile:    fmt.Sprintf("processed_tuples_%d.log", args.TaskID),
    	AckedLogFile:        fmt.Sprintf("acked_tuples_%d.log", args.TaskID),
	}
	hydfs.HandleCreate(node, "../emptyfile.txt", ts.ProcessedLogFile)
	hydfs.HandleCreate(node, "../emptyfile.txt", ts.AckedLogFile)


	tasks[args.TaskID] = ts
	fmt.Printf("Assigned task: %s\n", args.Exe)

	// Start goroutine to process tuples
	go processTuples(ts)
	go monitorAcks(ts)

	*reply = true
	return nil
}

func monitorAcks(ts *TaskState) {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    
    for range ticker.C {
        // Make a copy of unacked tuples to avoid holding lock during RPC
        unackedTuples := []rss.Tuple{}
        
        ts.mu1.RLock()
        ts.mu2.RLock()
        for key, tuple := range ts.ProcessedTuples {
            if _, acked := ts.AckedTuples[key]; !acked {
                unackedTuples = append(unackedTuples, tuple)
            }
        }
        ts.mu2.RUnlock()
        ts.mu1.RUnlock()
        
        // Now resend without holding locks
        for _, tuple := range unackedTuples {
            // Double-check it's still unacked (might have been acked during copy)
            ts.mu2.RLock()
            _, acked := ts.AckedTuples[tuple.Key]
            ts.mu2.RUnlock()
            
            if acked {
                continue // Got acked while we were copying, skip it
            }
            
            log.Printf("Task %d: Tuple %s not acked, resending", ts.ID, tuple.Key)
            
            if len(ts.Downstream) == 0 {
                continue // Final stage, nothing to resend to
            }
            
            idx := int(HashKey(tuple.Key)) % len(ts.Downstream)
            target := ts.Downstream[idx]
            
            var dummyReply bool
            err := sendRPC(target.IP, "Worker.AddTuples",
                &rss.AddTuplesArgs{
                    TaskID:     target.TaskID,
                    Tuples:     []rss.Tuple{tuple},
                    SourceIP:   getLocalIP() + ":9300",
                    SourceTask: ts.ID,
                },
                &dummyReply,
            )
            
            if err != nil {
                log.Printf("Task %d: Failed to resend tuple %s: %v", ts.ID, tuple.Key, err)
                // Could notify leader of downstream failure here
            }
        }
    }
}

// Main processing goroutine - reads from queue, processes, acks, and forwards
func processTuples(ts *TaskState) {
	// Create a scanner to read from stdout
	scanner := bufio.NewScanner(ts.Stdout)
	
	for tupleWithSource := range ts.InputQueue {
		tuple := tupleWithSource.Tuple
		
		// Check for duplicates (idempotency)
		ts.mu1.RLock()
		if _, exists := ts.ProcessedTuples[tuple.Key]; exists {
			ts.mu1.RUnlock()
			// Already processed, send ack anyway
			sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
			continue
		}
		ts.mu1.RUnlock()

		// Write tuple to operator's stdin
		_, err := fmt.Fprintf(ts.Stdin, "%s\t%s\n", tuple.Key, tuple.Value)
		if err != nil {
			log.Printf("Error writing to stdin for task %d: %v", ts.ID, err)
			continue
		}

		// Read output from operator's stdout
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Printf("Error reading from stdout for task %d: %v", ts.ID, err)
			}
			break
		}

		outputLine := scanner.Text()
		outputTuple := parseTuple(outputLine)

		// Mark as processed
		ts.mu1.Lock()
		ts.ProcessedTuples[tuple.Key] = tuple
		ts.mu1.Unlock()

		line := fmt.Sprintf("%s\t%s\n", tuple.Key, tuple.Value)
		hydfs.HandleAppendString(node, line, ts.ProcessedLogFile)

		// Handle final stage vs intermediate stage
		if len(ts.Downstream) == 0 {
			// Final stage - write to HyDFS
			fmt.Printf("%s\t%s\n", outputTuple.Key, outputTuple.Value)
			
			lineOutput := fmt.Sprintf("%s\t%s\n", outputTuple.Key, outputTuple.Value)
			os.WriteFile(ts.LastStageOutputFile, []byte(lineOutput), 0644)
			hydfs.HandleAppend(node, ts.LastStageOutputFile, ts.HydfsDest)
			os.WriteFile(ts.LastStageOutputFile, []byte{}, 0644)
			
			// Send ack back to source
			sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
		} else {
			// Intermediate stage - forward to downstream
			idx := int(HashKey(outputTuple.Key)) % len(ts.Downstream)
			target := ts.Downstream[idx]

			// Forward with retry logic
			for {
				var dummyReply bool
				err := sendRPC(target.IP, "Worker.AddTuples",
					&rss.AddTuplesArgs{
						TaskID:     target.TaskID,
						Tuples:     []rss.Tuple{outputTuple},
						SourceIP:   getLocalIP() + ":9300", // Worker's IP with port for ack
						SourceTask: ts.ID,
					},
					&dummyReply,
				)
				
				if err == nil {
					// Successfully forwarded, now send ack back to our source
					sendAck(tupleWithSource.SourceIP, tupleWithSource.SourceTask, tuple)
					break
				}

				// RPC failed - in production, notify leader and wait for reassignment
				log.Printf("Failed to forward tuple to %s, retrying...", target.IP)
				// For now, just retry (you'll need proper failure handling)
				// notifyLeaderTaskFailed(target.TaskID)
				// Wait for leader to update downstream info
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

	// Mark tuple as acknowledged
	ts.mu2.Lock()
	ts.AckedTuples[args.Tuple.Key] = args.Tuple
	ts.mu2.Unlock()

	line := fmt.Sprintf("%s\t%s\n", args.Tuple.Key, args.Tuple.Value)
	hydfs.HandleAppendString(node, line, ts.AckedLogFile)

	fmt.Printf("Task %d received ack for tuple: %s\n", args.TaskID, args.Tuple.Key)
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

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}