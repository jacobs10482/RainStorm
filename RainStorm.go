package main


import (
	hydfs "g51mp4/hydfs_system"
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"sync"
)

// --------------------------
// Task structures
// --------------------------

type Tuple struct {
	Key   string
	Value string
}

type AssignTaskArgs struct {
	TaskID int
	Stage  int
	Exe    string
	Args   []string
}

type AddTuplesArgs struct {
	TaskID int
	Tuples []Tuple
}

type KillTaskArgs struct {
	TaskID int
}

type TaskState struct {
	ID     int
	Stage  int
	Cmd    *exec.Cmd
	Stdin  io.WriteCloser
	Stdout io.ReadCloser
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

func (w *Worker) AssignTask(args *AssignTaskArgs, reply *bool) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	cmd := exec.Command(args.Exe, args.Args...)

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
		Stdin:  stdin,
		Stdout: stdout,
	}

	tasks[args.TaskID] = ts

	// Start goroutine to read task output
	go func() {
		scanner := bufio.NewScanner(ts.Stdout)
		for scanner.Scan() {
			line := scanner.Text()
			// TODO: send this line back to leader
			fmt.Printf("[Task %d Output] %s\n", ts.ID, line)
		}
	}()

	*reply = true
	return nil
}

func (w *Worker) AddTuples(args *AddTuplesArgs, reply *bool) error {
	tasksMu.Lock()
	ts, ok := tasks[args.TaskID]
	tasksMu.Unlock()
	if !ok {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	for _, t := range args.Tuples {
		fmt.Fprintf(ts.Stdin, "%s %s\n", t.Key, t.Value)
	}

	*reply = true
	return nil
}

func (w *Worker) KillTask(args *KillTaskArgs, reply *bool) error {
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

// --------------------------
// Main RPC listener
// --------------------------

func main() {
	
	hydfs.Start()


	port := "9300" // Or read from command-line arguments

	worker := &Worker{}
	rpc.Register(worker)

	ln, err := net.Listen("tcp", ":"+port)
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
