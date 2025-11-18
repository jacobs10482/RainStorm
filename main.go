package main

import (
	"bufio"
	"os"
	"strings"
	"fmt"
	"log"
	"sort"
	"crypto/sha256"
	fd "g51mp4/failure_detection"
	"net/rpc"
	"sync"
	"net"
	"time"
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

var vmMapFD = map[string]string{
    "172.22.95.98:9000":  "vm1",
    "172.22.154.169:9000":  "vm2",
    "172.22.158.169:9000":  "vm3",
    "172.22.95.99:9000":  "vm4",
    "172.22.154.170:9000":  "vm5",
    "172.22.158.170:9000":  "vm6",
    "172.22.95.100:9000":  "vm7",
    "172.22.154.171:9000":  "vm8",
    "172.22.158.171:9000":  "vm9",
    "172.22.95.101:9000":  "vm10",
}

type NodeRPC struct {
    node *fd.Node // pointer to the actual Node containing membership, heartbeat, etc.
}

// Reply for the CreateFile RPC
type CreateFileReply struct {
    Success bool
    Message string
}

// Args for the CreateFile RPC
type CreateFileArgs struct {
    Filename       string   // The name of the file
    Data           []byte   // File contents
    IsPrimary      bool     // Whether this node is the main replica
    OtherReplicas  []string // Addresses of the other replicas (can be empty)
}

// Args for the Append RPC
type AppendFileArgs struct {
	Filename      string   // HyDFS filename
	Data          []byte   // Data to append
	IsPrimary     bool     // True if this node is the coordinator (primary)
	OtherReplicas []string // Backup replicas to forward append
}

// Reply for the Append RPC
type AppendFileReply struct {
	Success bool
	Message string
}

// Args for the GetFile RPC
type GetFileArgs struct {
    Filename  string // The HyDFS filename
}

// Reply for the GetFile RPC
type GetFileReply struct {
    Success bool
    Data    []byte // File data (for RPC transfer)
    Message string
}

// Args for the MultiAppend RPC
type MultiAppendArgs struct {
    LocalFile string
    HydfsFile string
}

// Reply for the MultiAppend RPC
type MultiAppendReply struct {
    Success bool
    Message string
}

// Args for the MergeFile RPC
type MergeFileArgs struct {
    Filename string
}

// Reply for the MergeFile RPC
type MergeFileReply struct {
    Success bool
    Message string
}



// File-level locks to prevent concurrent writes
var fileLocks = struct {
	sync.Mutex
	m map[string]*sync.Mutex
}{m: make(map[string]*sync.Mutex)}

// getRPCAddr derives the RPC address from the node's address (IP:port).
// It adds +100 to the existing port number (or defaults to 9200 if parse fails).
func getRPCAddrFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// if addr has no port, just use it directly
		return net.JoinHostPort(addr, "9200")
	}
	return net.JoinHostPort(host, "9200")
}

// StartRPCListener starts the RPC listener for the node
func StartRPCListener(node *fd.Node) {
	go func() {
		rpcObj := &NodeRPC{node: node}
		if err := rpc.Register(rpcObj); err != nil {
			log.Fatalf("Failed to register RPC object: %v", err)
		}

		rpcAddr := getRPCAddrFromAddr(node.Addr)
		listener, err := net.Listen("tcp", rpcAddr)
		if err != nil {
			log.Fatalf("Failed to start RPC listener on %s: %v", rpcAddr, err)
		}
		defer listener.Close()

		log.Printf("RPC server listening on %s\n", rpcAddr)

		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("RPC accept error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

// getFileLock gets a file lock for the given filename
func getFileLock(filename string) *sync.Mutex {
	fileLocks.Lock()
	defer fileLocks.Unlock()

	lk, exists := fileLocks.m[filename]
	if !exists {
		lk = &sync.Mutex{}
		fileLocks.m[filename] = lk
	}
	return lk
}


// CreateFile creates a new file on the node
func (r *NodeRPC) CreateFile(args CreateFileArgs, reply *CreateFileReply) error {
	lock := getFileLock(args.Filename)
	lock.Lock()
	defer lock.Unlock()

    // Check if file already exists
    if _, err := os.Stat(args.Filename); err == nil {
        reply.Success = false
        reply.Message = fmt.Sprintf("File %s already exists on %s", args.Filename, r.node.Addr)
        return nil
    } else if !os.IsNotExist(err) {
        reply.Success = false
        reply.Message = fmt.Sprintf("Error checking file %s: %v", args.Filename, err)
        return err
    }

    // Save the file locally
    err := os.WriteFile(args.Filename, args.Data, 0644)
    if err != nil {
        reply.Success = false
        reply.Message = fmt.Sprintf("Failed to write file %s: %v", args.Filename, err)
        return err
    }

    // If primary, forward to replicas and wait for ACKs
    if args.IsPrimary && len(args.OtherReplicas) > 0 {
        allAck := true
        for _, addr := range args.OtherReplicas {
            client, err := rpc.Dial("tcp", addr)
            if err != nil {
                log.Printf("Failed to connect to replica %s: %v\n", addr, err)
                allAck = false
                continue
            }

            replicaArgs := CreateFileArgs{
                Filename:      args.Filename,
                Data:          args.Data,
                IsPrimary:     false, // avoid forwarding again
                OtherReplicas: nil,
            }
            var replicaReply CreateFileReply
            err = client.Call("NodeRPC.CreateFile", replicaArgs, &replicaReply)
            if err != nil || !replicaReply.Success {
                log.Printf("Replication to %s failed: %v\n", addr, err)
                allAck = false
            } else {
                fmt.Printf("Replicated %s to %s\n", args.Filename, addr)
            }
            client.Close()
        }

        if allAck {
            reply.Success = true
            reply.Message = fmt.Sprintf("File %s saved and replicated to all replicas", args.Filename)
        } else {
            reply.Success = false
            reply.Message = fmt.Sprintf("File %s saved locally but some replicas failed", args.Filename)
        }

    } else {
        // Secondary replicas just save the file locally
        reply.Success = true
        reply.Message = fmt.Sprintf("File %s saved on %s", args.Filename, r.node.Addr)
    }

    return nil
}

// AppendFile appends data to an existing file on HyDFS
func (r *NodeRPC) AppendFile(args AppendFileArgs, reply *AppendFileReply) error {
	log.Printf("[Replica %s] START append request for %s (primary=%v)\n",
        r.node.Addr, args.Filename, args.IsPrimary)
	lock := getFileLock(args.Filename)
	lock.Lock()
	defer lock.Unlock()

	// Check that the file exists
	if _, err := os.Stat(args.Filename); os.IsNotExist(err) {
		reply.Success = false
		reply.Message = fmt.Sprintf("File %s does not exist on %s", args.Filename, r.node.Addr)
		return nil
	}

	// Append data locally
	f, err := os.OpenFile(args.Filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("Error opening file %s: %v", args.Filename, err)
		return err
	}
	defer f.Close()

	_, err = f.Write(args.Data)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("Failed to append to %s: %v", args.Filename, err)
		return err
	}

	// If primary, replicate to backups
	if args.IsPrimary && len(args.OtherReplicas) > 0 {
		allAck := true
		for _, addr := range args.OtherReplicas {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Failed to connect to replica %s: %v\n", addr, err)
				allAck = false
				continue
			}

			replicaArgs := AppendFileArgs{
				Filename:      args.Filename,
				Data:          args.Data,
				IsPrimary:     false,
				OtherReplicas: nil,
			}
			var replicaReply AppendFileReply
			err = client.Call("NodeRPC.AppendFile", replicaArgs, &replicaReply)
			client.Close()

			if err != nil || !replicaReply.Success {
				log.Printf("Replication append to %s failed: %v\n", addr, err)
				allAck = false
			} else {
				log.Printf("Replicated append %s to %s\n", args.Filename, addr)
			}
		}

		if allAck {
			reply.Success = true
			reply.Message = fmt.Sprintf("Append to %s completed on all replicas", args.Filename)
		} else {
			reply.Success = false
			reply.Message = fmt.Sprintf("Append to %s completed locally but some replicas failed", args.Filename)
		}
	} else {
		// Secondary just acknowledges
		reply.Success = true
		reply.Message = fmt.Sprintf("Appended to %s on replica %s", args.Filename, r.node.Addr)
	}

	fmt.Printf("[Replica %s] END append for %s successfully completed\n\n",
        r.node.Addr, args.Filename)

	return nil
}

// GetFile gets a file from the primary replica
func (r *NodeRPC) GetFile(args GetFileArgs, reply *GetFileReply) error {
	lock := getFileLock(args.Filename)
	lock.Lock()
	defer lock.Unlock()

	// Check if file exists
	data, err := os.ReadFile(args.Filename)
	if err != nil {
		if os.IsNotExist(err) {
			reply.Success = false
			reply.Message = fmt.Sprintf("File %s not found on %s", args.Filename, r.node.Addr)
			return nil
		}
		reply.Success = false
		reply.Message = fmt.Sprintf("Error reading file %s on %s: %v", args.Filename, r.node.Addr, err)
		return err
	}

	// Return file contents
	reply.Success = true
	reply.Data = data
	reply.Message = fmt.Sprintf("File %s successfully read from %s", args.Filename, r.node.Addr)
	return nil
}

// MergeFile merges a file from all replicas
func (r *NodeRPC) MergeFile(args MergeFileArgs, reply *MergeFileReply) error {
    log.Printf("[Replica %s] START merge for %s\n", r.node.Addr, args.Filename)

    // Get replicas for this file
    replicas := getReplicas(r.node, args.Filename, 3)
    if len(replicas) == 0 {
        reply.Success = false
        reply.Message = "No replicas found"
        return nil
    }

    var mergedData []byte

    // Collect file data from all replicas (including self)
    for _, nodeID := range replicas {
        rpcAddr := getRPCAddrFromNodeID(nodeID)

        // Handle local replica directly (no self-RPC)
        if rpcAddr == getRPCAddrFromAddr(r.node.Addr) {
            data, err := os.ReadFile(args.Filename)
            if err != nil {
                log.Printf("[Replica %s] Failed to read local file: %v\n", r.node.Addr, err)
                continue
            }
            if len(data) > len(mergedData) {
                mergedData = data
            }
            continue
        }

        // Fetch remote replica’s file
        client, err := rpc.Dial("tcp", rpcAddr)
        if err != nil {
            log.Printf("[Replica %s] Failed to connect to %s: %v\n", r.node.Addr, rpcAddr, err)
            continue
        }

        var remoteReply struct {
            Success bool
            Data    []byte
        }
        err = client.Call("NodeRPC.GetLocalFile", args, &remoteReply)
        client.Close()

        if err != nil || !remoteReply.Success {
            log.Printf("[Replica %s] Failed to get file from %s: %v\n", r.node.Addr, rpcAddr, err)
            continue
        }

        if len(remoteReply.Data) > len(mergedData) {
            mergedData = remoteReply.Data
        }
    }

    // Write merged data locally
    lock := getFileLock(args.Filename)
    lock.Lock()
    err := os.WriteFile(args.Filename, mergedData, 0644)
    lock.Unlock()
    if err != nil {
        reply.Success = false
        reply.Message = fmt.Sprintf("Failed to write merged file: %v", err)
        return err
    }

    // Replicate merged data to all other replicas
    for _, nodeID := range replicas {
        rpcAddr := getRPCAddrFromNodeID(nodeID)
        if rpcAddr == getRPCAddrFromAddr(r.node.Addr) {
            continue // skip self
        }

        client, err := rpc.Dial("tcp", rpcAddr)
        if err != nil {
            log.Printf("[Replica %s] Failed to connect to %s: %v\n", r.node.Addr, rpcAddr, err)
            continue
        }

        var replicaReply MergeFileReply
        err = client.Call("NodeRPC.ApplyMergedFile", struct {
            Filename string
            Data     []byte
        }{args.Filename, mergedData}, &replicaReply)
        client.Close()

        if err != nil || !replicaReply.Success {
            log.Printf("[Replica %s] Failed to send merged file to %s: %v\n", r.node.Addr, rpcAddr, err)
        } else {
            log.Printf("[Replica %s] Sent merged file to %s\n", r.node.Addr, rpcAddr)
        }
    }

    reply.Success = true
    reply.Message = fmt.Sprintf("Merge completed for %s", args.Filename)
    fmt.Printf("[Replica %s] END merge for %s\n", r.node.Addr, args.Filename)
    return nil
}

// GetLocalFile retrieves a file from the local replica
func (r *NodeRPC) GetLocalFile(args MergeFileArgs, reply *struct {
    Success bool
    Data    []byte
}) error {
    data, err := os.ReadFile(args.Filename)
    if err != nil {
        reply.Success = false
        return err
    }
    reply.Success = true
    reply.Data = data
    return nil
}

// ApplyMergedFile applies a merged file to the local replica
func (r *NodeRPC) ApplyMergedFile(args struct {
    Filename string
    Data     []byte
}, reply *MergeFileReply) error {
    lock := getFileLock(args.Filename)
    lock.Lock()
    defer lock.Unlock()

    err := os.WriteFile(args.Filename, args.Data, 0644)
    if err != nil {
        reply.Success = false
        reply.Message = fmt.Sprintf("Failed to apply merged file: %v", err)
        return err
    }

    log.Printf("[Replica %s] Applied merged file for %s\n", r.node.Addr, args.Filename)
    reply.Success = true
    reply.Message = "Merged file applied successfully"
    return nil
}


// hashString hashes a string to a place on the ring
func hashString(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 |
		uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
}

// getReplicas gets the replicas for a given key
func getReplicas(node *fd.Node, key string, replicationFactor int) []string {
	// Get current node IDs from the membership list (thread-safe)
	nodeIDs := node.Membership.NodeIDs()
	if len(nodeIDs) == 0 {
		return nil
	}

	hash := hashString(key)
	n := len(nodeIDs)

	// Sort nodes by hash for consistent clockwise ring traversal
	sortedNodes := make([]string, n)
	copy(sortedNodes, nodeIDs)
	sort.Slice(sortedNodes, func(i, j int) bool {
		return hashString(sortedNodes[i]) < hashString(sortedNodes[j])
	})

	// Find the first node clockwise
	idx := sort.Search(n, func(i int) bool {
		return hashString(sortedNodes[i]) >= hash
	})
	if idx == n {
		idx = 0 // wrap around the ring
	}

	// Collect replicationFactor nodes clockwise, wrapping around the ring
	replicas := make([]string, 0, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		replicas = append(replicas, sortedNodes[(idx+i)%n])
	}

	return replicas
}

// MultiAppend handles the multiappend RPC call
func (n *NodeRPC) MultiAppend(args MultiAppendArgs, reply *MultiAppendReply) error {
    handleAppend(n.node, args.LocalFile, args.HydfsFile)
    reply.Success = true
    reply.Message = fmt.Sprintf("Ran append on %s", n.node.ID)
    return nil
}

// getRPCAddrFromNodeID gets the RPC address from a node ID
func getRPCAddrFromNodeID(nodeID string) string {
	parts := strings.SplitN(nodeID, "-", 2)
	if len(parts) == 0 {
		return ""
	}
	hostPort := parts[0]

	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		// if no port or invalid format, just return with 9200
		return net.JoinHostPort(hostPort, "9200")
	}
	return net.JoinHostPort(host, "9200")
}


// handleCreate creates a new file on all replicas
func handleCreate(node *fd.Node, localFile, hydfsFile string) {
	// 1. Check if local file exists
	data, err := os.ReadFile(localFile)
	if err != nil {
		log.Printf("Error: cannot read local file %s (%v)\n", localFile, err)
		return
	}

	// 2. Pick replicas (3 total)
	replicas := getReplicas(node, hydfsFile, 3)
	if len(replicas) == 0 {
		log.Println("Error: no replicas available")
		return
	}

	primary := replicas[0]
	
	backupReplicas := make([]string, 0, len(replicas)-1)
	for _, nodeID := range replicas[1:] {
		backupReplicas = append(backupReplicas, getRPCAddrFromNodeID(nodeID))
	}


	log.Printf("Replicating %s to primary %s (backups: %v)\n", hydfsFile, primary, backupReplicas)

	// 3. Prepare RPC arguments
	args := CreateFileArgs{
		Filename:     hydfsFile,
		Data:         data,
		IsPrimary:    true,             // we're the client sending to primary
		OtherReplicas: backupReplicas,   // primary will handle replication
	}

	var reply CreateFileReply

	// 4. Dial the primary replica (use its RPC port)
	rpcAddr := getRPCAddrFromNodeID(primary)
	client, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		log.Printf("RPC dial error to %s: %v\n", rpcAddr, err)
		return
	}
	defer client.Close()

	// 5. Perform the RPC call
	err = client.Call("NodeRPC.CreateFile", args, &reply)
	if err != nil {
		fmt.Printf("RPC error: %v\n", err)
		return
	}

	// 6. Report result
	if reply.Success {
		log.Printf("File '%s' created successfully on %s\n", hydfsFile, primary)
	} else {
		log.Printf("Failed to create '%s': %s\n", hydfsFile, reply.Message)
	}
}

// handleAppend appends data to all replicas of a given HyDFS file
func handleAppend(node *fd.Node, localFile, hydfsFile string) {
	// Read data from local file
	data, err := os.ReadFile(localFile)
	if err != nil {
		log.Printf("Error: cannot read local file %s (%v)\n", localFile, err)
		return
	}

	// Find the same replicas responsible for this HyDFS file
	replicas := getReplicas(node, hydfsFile, 3)
	if len(replicas) == 0 {
		log.Println("Error: no replicas available")
		return
	}

	primary := replicas[0]
	backups := make([]string, 0, len(replicas)-1)
	for _, nodeID := range replicas[1:] {
		backups = append(backups, getRPCAddrFromNodeID(nodeID))
	}

	log.Printf("Appending %s to primary %s (backups: %v)\n", hydfsFile, primary, backups)

	args := AppendFileArgs{
		Filename:      hydfsFile,
		Data:          data,
		IsPrimary:     true,
		OtherReplicas: backups,
	}

	var reply AppendFileReply

	rpcAddr := getRPCAddrFromNodeID(primary)
	client, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		log.Printf("RPC dial error to %s: %v\n", rpcAddr, err)
		return
	}
	defer client.Close()

	err = client.Call("NodeRPC.AppendFile", args, &reply)
	if err != nil {
		log.Printf("RPC error: %v\n", err)
		return
	}

	if reply.Success {
		log.Printf("Append to '%s' succeeded.\n", hydfsFile)
	} else {
		log.Printf("Append to '%s' failed: %s\n", hydfsFile, reply.Message)
	}
}

// handleGet fetches a file from the primary replica and saves it to a local file
func handleGet(node *fd.Node, hydfsFile, localFile string) {
	// Determine the primary replica for this file
	replicas := getReplicas(node, hydfsFile, 3)
	if len(replicas) == 0 {
		fmt.Println("Error: no replicas available")
		return
	}

	primary := replicas[0]
	rpcAddr := getRPCAddrFromNodeID(primary)

	log.Printf("Fetching %s from primary %s → saving to %s\n", hydfsFile, primary, localFile)

	// Prepare RPC arguments
	args := GetFileArgs{Filename: hydfsFile}
	var reply GetFileReply

	// Connect to primary replica
	client, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		log.Printf("RPC dial error to %s: %v\n", rpcAddr, err)
		return
	}
	defer client.Close()

	// Perform the RPC call
	err = client.Call("NodeRPC.GetFile", args, &reply)
	if err != nil {
		log.Printf("RPC error: %v\n", err)
		return
	}

	// Handle the result
	if !reply.Success {
		log.Printf("Failed to get '%s': %s\n", hydfsFile, reply.Message)
		return
	}

	// Save the fetched data locally
	err = os.WriteFile(localFile, reply.Data, 0644)
	if err != nil {
		log.Printf("Error writing local file %s: %v\n", localFile, err)
		return
	}

	log.Printf("File '%s' successfully fetched from %s and saved as '%s'\n", hydfsFile, rpcAddr, localFile)
}

// handleGetFromReplica fetches a file from a replica and saves it to a local file
func handleGetFromReplica(replicaIP, hydfsFile, localFile string) {
	rpcAddr := fmt.Sprintf("%s:9200", replicaIP)

	log.Printf("Fetching %s from replica %s → saving to %s\n", hydfsFile, rpcAddr, localFile)

	// Prepare RPC arguments
	args := GetFileArgs{Filename: hydfsFile}
	var reply GetFileReply

	// Dial replica
	client, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		fmt.Printf("RPC dial error to %s: %v\n", rpcAddr, err)
		return
	}
	defer client.Close()

	// Perform RPC call
	err = client.Call("NodeRPC.GetFile", args, &reply)
	if err != nil {
		fmt.Printf("RPC error: %v\n", err)
		return
	}

	// Handle reply
	if !reply.Success {
		log.Printf("Failed to get '%s' from %s: %s\n", hydfsFile, rpcAddr, reply.Message)
		return
	}

	// Save file locally
	err = os.WriteFile(localFile, reply.Data, 0644)
	if err != nil {
		fmt.Printf("Error writing local file %s: %v\n", localFile, err)
		return
	}

	log.Printf("File '%s' successfully fetched from replica %s and saved as '%s'\n",
		hydfsFile, rpcAddr, localFile)
}

// nodeIDToVMName converts a node ID to a VM name
func nodeIDToVMName(nodeID string) string {
    parts := strings.SplitN(nodeID, "-", 2)  // split at timestamp
    ipPort := parts[0]                        
    if vmName, ok := vmMapFD[ipPort]; ok {
        return vmName
    }
    return ipPort 
}


// handleMultiAppend launches appends from VMi,…VMj simultaneously to HyDFSfilename
func handleMultiAppend(hydfsFile string, vmFiles map[string]string) {
	var wg sync.WaitGroup
	for vmAddr, localFile := range vmFiles {
		wg.Add(1)
		go func(addr, file string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("[multiappend] Error dialing %s: %v\n", addr, err)
				return
			}
			defer client.Close()

			args := MultiAppendArgs{
				LocalFile:  file,
				HydfsFile:  hydfsFile,
			}
			var reply MultiAppendReply
			err = client.Call("NodeRPC.MultiAppend", args, &reply)
			if err != nil {
				fmt.Printf("[multiappend] RPC error on %s: %v\n", addr, err)
				return
			}

			if reply.Success {
				fmt.Printf("[multiappend] %s attemped append on %s\n", file, addr)
			} else {
				fmt.Printf("[multiappend] %s failed on %s: %s\n", file, addr, reply.Message)
			}
		}(vmAddr, localFile)
	}

	wg.Wait()
	log.Printf("[multiappend] All appends completed for %s\n", hydfsFile)
}

// handleLS lists the replicas for a given HyDFS file
func handleLS(node *fd.Node, hydfsFile string) {
	// Compute file ID
	fileID := hashString(hydfsFile)

	// Get replicas responsible for this file
	replicas := getReplicas(node, hydfsFile, 3)
	if len(replicas) == 0 {
		fmt.Println("Error: no replicas available")
		return
	}

	fmt.Printf("FileID for '%s': %d\n", hydfsFile, fileID)
	fmt.Println("Replicas storing this file:")

	// For each replica, compute its node ID and print info
	for _, replica := range replicas {
		nodeID := hashString(replica)
		VMName := nodeIDToVMName(replica)
		addr := replica
		if parts := strings.Split(replica, "-"); len(parts) > 0 {
			addr = parts[0]
		}

		fmt.Printf("  - %s (VMName: %s, NodeID: %d)\n", addr, VMName, nodeID)
	}
}

// handleFailedNode handles the case where a node fails (find new replica)
func handleFailedNode(node *fd.Node, _ string) {
	// List all files in the current directory
	files, err := os.ReadDir(".")
	if err != nil {
		log.Printf("[rebalancer] Failed to read local directory: %v\n", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()

		// Determine replicas
		replicas := getReplicas(node, filename, 3)

		// Only act if this node is the primary
		if replicas[0] != node.ID {
			continue
		}


		// Read file data
		data, err := os.ReadFile(filename)
		if err != nil {
			fmt.Printf("[rebalancer] Failed to read file %s: %v\n", filename, err)
			continue
		}

		// Replicate to secondary replicas
		for _, id := range replicas[1:] {
			log.Printf("Handling rereplication of %s (failure)", file.Name())
			addr := getRPCAddrFromNodeID(id)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("[rebalancer] Failed to connect to replica %s: %v\n", addr, err)
				continue
			}

			args := CreateFileArgs{
				Filename:      filename,
				Data:          data,
				IsPrimary:     false,
				OtherReplicas: nil,
			}
			var reply CreateFileReply
			err = client.Call("NodeRPC.CreateFile", args, &reply)
			if err != nil || !reply.Success {
				log.Printf("[rebalancer] Replication to %s failed for %s: %v\n", addr, filename, err)
			} else {
				log.Printf("[rebalancer] Replicated %s to %s\n", filename, addr)
			}
			client.Close()
		}
	}
}

// handleNewNode handles the case where a new node joins (rebalancing)
func handleNewNode(node *fd.Node, newNode string) {

	time.Sleep(4 * time.Second) //
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Printf("[rebalancer] Failed to read local directory: %v\n", err)
		return
	}


	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()

		// Compute replicas for this file with the new membership
		replicas := getReplicas(node, filename, 3)

		// Check if this node is still a replica
		isReplica := false
		for _, id := range replicas {
			if id == node.ID {
				isReplica = true
				break
			}
		}

		if !isReplica {
			// Not responsible anymore
			fmt.Printf("[rebalancer] Removing %s (no longer a replica)\n", filename)
			if err := os.Remove(filename); err != nil {
				fmt.Printf("[rebalancer] Failed to remove %s: %v\n", filename, err)
			}
			continue
		}

		
		
		// Check if newNode.ID is one of the replicas
		needsReplication := false
		for _, id := range replicas {
			if id == newNode {
				needsReplication = true
				break
			}
		}
		if needsReplication {
			log.Printf("Handling rereplication of %s (join)", file.Name())
			data, err := os.ReadFile(filename)
			if err != nil {
				fmt.Printf("[rebalancer] Failed to read file %s: %v\n", filename, err)
				continue
			}

			addr := getRPCAddrFromNodeID(newNode)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("[rebalancer] Failed to connect to new node %s: %v\n", addr, err)
				continue
			}

			args := CreateFileArgs{
				Filename:      filename,
				Data:          data,
				IsPrimary:     false,
				OtherReplicas: nil,
			}
			var reply CreateFileReply
			err = client.Call("NodeRPC.CreateFile", args, &reply)
			if err != nil || !reply.Success {
				fmt.Printf("Failed to replicate %s to new node %s: %v\n", filename, addr, err)
			} else {
				fmt.Printf("Replicated %s to new node %s\n", filename, addr)
			}
			client.Close()
		}
	
	}
}


// listLocalStore lists the files in the local HyDFS store
func listLocalStore() {
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error reading HyDFS directory:", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("HyDFS store is empty.")
		return
	}

	fmt.Println("Files in HyDFS store:")
	for _, file := range files {
		if !file.IsDir() {
			fmt.Println("  -", file.Name())
		}
	}
}

// handleListMemIds lists the membership IDs
func handleListMemIds(node *fd.Node) {
	members := node.Membership.NodeIDs()

	// Precompute ring hashes
	type entry struct {
		VMName string
		NodeID string
		RingID uint64
	}
	nodes := make([]entry, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, entry{
			VMName: nodeIDToVMName(m),
			NodeID: m,
			RingID: hashString(m),
		})
	}

	// Sort by RingID (ascending order on the hash ring)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].RingID < nodes[j].RingID
	})

	// Print in sorted order
	fmt.Println("Membership List (VMName | NodeID | RingID):")
	for _, n := range nodes {
		fmt.Printf("  %s | %s | %d\n", n.VMName, n.NodeID, n.RingID)
	}
}

// StdinListener listens for commands from the user
func StdinListener(node *fd.Node) {
	scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if line == "" {
            continue
        }
        args := strings.Fields(line)
        cmd := args[0]

        failurecmd := node.Responder(cmd)
        if failurecmd {
            continue
        }

        switch cmd {
        case "create":
            if len(args) != 3 {
                fmt.Println("Usage: create <localfilename> <HyDFSfilename>")
                continue
            }
            localFile := args[1]
			hyDFSFile := args[2]
			fmt.Printf("[create] %s -> %s\n", localFile, hyDFSFile)
			handleCreate(node, localFile, hyDFSFile)

            // TODO: call your create handler

        case "get":
			if len(args) != 3 {
				fmt.Println("Usage: get <HyDFSfilename> <localfilename>")
				continue
			}
		
			hydfsFile := args[1]
			localFile := args[2]
		
			fmt.Printf("[get] %s -> %s\n", hydfsFile, localFile)
			handleGet(node, hydfsFile, localFile)
		

        case "append":
			if len(args) != 3 {
				fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
				continue
			}
			localFile := args[1]
			hyDFSFile := args[2]
			fmt.Printf("[append] %s -> %s\n", localFile, hyDFSFile)
			handleAppend(node, localFile, hyDFSFile)
		
        case "merge":
			if len(args) != 2 {
				fmt.Println("Usage: merge <HyDFSfilename>")
				continue
			}
			hydfsFile := args[1]
			fmt.Printf("[merge] Starting merge for %s\n", hydfsFile)
		
			// Merge on primary
			replicas := getReplicas(node, hydfsFile, 3)
			if len(replicas) == 0 {
				fmt.Println("No replicas found")
				continue
			}
		
			primary := getRPCAddrFromNodeID(replicas[0])
			client, err := rpc.Dial("tcp", primary)
			if err != nil {
				fmt.Printf("RPC dial error to %s: %v\n", primary, err)
				continue
			}
		
			var reply MergeFileReply
			err = client.Call("NodeRPC.MergeFile", MergeFileArgs{Filename: hydfsFile}, &reply)
			client.Close()
			if err != nil {
				fmt.Printf("RPC error: %v\n", err)
				continue
			}
			fmt.Println(reply.Message)
		
			
        case "ls":
            if len(args) != 2 {
                fmt.Println("Usage: ls <HyDFSfilename>")
                continue
            }
			hyDFSFile := args[1]
            handleLS(node, hyDFSFile)
            // TODO: ls handler

        case "liststore":
            listLocalStore()
            continue

		case "getfromreplica":
			if len(args) != 4 {
				fmt.Println("Usage: getfromreplica <VMaddr> <HyDFSfilename> <localfilename>")
				continue
			}
		
			replicaIP := args[1]
			hydfsFile := args[2]
			localFile := args[3]
		
			fmt.Printf("[getfromreplica] %s %s -> %s\n", replicaIP, hydfsFile, localFile)
			handleGetFromReplica(replicaIP, hydfsFile, localFile)
		
        case "list_mem_ids":
            handleListMemIds(node)
            continue

        case "multiappend":
			/*
			   Usage:
			   multiappend <HyDFSfilename> <vm1> <vm2> ... <localfile1> <localfile2> ...
			   Example:
			   multiappend report.txt vm1 vm2 part1.txt part2.txt
			*/
			if len(args) < 4 {
				fmt.Println("Usage: multiappend <HyDFSfilename> <vm1> <vm2> ... <localfile1> <localfile2> ...")
				continue
			}
		
			hydfsFile := args[1]
			half := (len(args) - 2) / 2
			vmNames := args[2 : 2+half]
			localFiles := args[2+half:]
		
			if len(vmNames) != len(localFiles) {
				fmt.Println("Error: number of VMs and local files must match")
				continue
			}
		
			vmFiles := make(map[string]string)
			for i := range vmNames {
				addr, ok := vmMapRPC[vmNames[i]]
				if !ok {
					fmt.Printf("Error: unknown VM name '%s'\n", vmNames[i])
					continue
				}
				vmFiles[addr] = localFiles[i]
			}
		
			fmt.Printf("[multiappend] %s: launching appends from %v\n", hydfsFile, vmNames)
			handleMultiAppend(hydfsFile, vmFiles)

            // TODO: multiappend handler
		case "testreplicas":
			// Just print replicas for a test key "helloworld"
			replicas := getReplicas(node, "helloworld", 3)
			fmt.Println("Replicas for 'helloworld':", replicas)
	
        default:
            fmt.Println("Unknown command. Available commands:")
            fmt.Println("list_mem, list_self, leave, display_suspects")
            fmt.Println("create, get, append, merge, ls, liststore, getfromreplica, list_mem_ids, multiappend")
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading standard input:", err)
    }

}

// initHyDFSDir initializes the HyDFS directory
func initHyDFSDir() (string, error) {
	// Get current directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %v", err)
	}

	// Path to HyDFS directory
	hyDFSPath := cwd + "/HyDFS"

	// If the folder already exists, remove it completely
	if _, err := os.Stat(hyDFSPath); err == nil {
		fmt.Println("🧹 Removing old HyDFS directory...")
		if err := os.RemoveAll(hyDFSPath); err != nil {
			return "", fmt.Errorf("failed to remove old HyDFS directory: %v", err)
		}
	}

	// Create a fresh HyDFS directory
	err = os.Mkdir(hyDFSPath, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create HyDFS directory: %v", err)
	}

	// Change into that directory
	err = os.Chdir(hyDFSPath)
	if err != nil {
		return "", fmt.Errorf("failed to cd into HyDFS: %v", err)
	}

	fmt.Println("📁 Created fresh HyDFS directory at:", hyDFSPath)
	return hyDFSPath, nil
}

// drainChannel drains the channel
func drainChannel(ch chan string) {
    for {
        select {
        case <-ch:
            // discard the value
        default:
            // channel is empty, stop draining
            return
        }
    }
}


// main initializes the failure detection and starts the RPC listener
func main() {

	node := fd.InitializeFailureDetection()

	hyDFSPath, err := initHyDFSDir()
	if err != nil {
		log.Fatalf("Failed to initialize HyDFS directory: %v", err)
	}

	fmt.Println("HyDFS active directory:", hyDFSPath)


	StartRPCListener(node)

	go func() {
		for deadNode := range node.FailCh {
			
			handleFailedNode(node, deadNode)
		}
	}()
	
	time.Sleep(5 * time.Second)

	drainChannel(node.JoinCh)

	go func() {
		for newNode := range node.JoinCh {
			
			handleNewNode(node, newNode)
		}
	}()


	StdinListener(node)
	
	
	select {}
}
