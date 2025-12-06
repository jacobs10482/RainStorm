package failure_detection

import (
	"bufio"
	"encoding/json"
	"fmt"
	//"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Status represents the current state of a node in the cluster
type Status string

const (
	Alive   Status = "Alive"
	Suspect Status = "Suspect"
	Dead    Status = "Dead"
)

type Member struct {
	NodeID        string    // Unique identifier for this node
	Addr          string    // Network address (IP:port) of this node
	Status        Status    // Current status (Alive, Suspect, Dead)
	Incarnation   int       // Version number to handle node resurrection
	LastHeartbeat time.Time // Timestamp of last received heartbeat
	SuspectTimer  time.Time // Timestamp when node was marked as suspect
}

type MembershipList struct {
	mu      sync.Mutex         // Mutex for thread-safe access to members map
	members map[string]*Member // Map of node ID to member information
}

func NewMembershipList() *MembershipList {
	return &MembershipList{members: make(map[string]*Member)}
}

// AddOrUpdate adds a new member to the membership list or updates an existing one
// Uses incarnation numbers to handle node resurrection and prevent stale updates
func (n *Node) AddOrUpdate(m *Member) {
	ml := n.Membership
	ml.mu.Lock()
	defer ml.mu.Unlock()

	existing, exists := ml.members[m.NodeID]

	// Create updated member with current timestamp
	updated := &Member{
		NodeID:        m.NodeID,
		Addr:          m.Addr,
		Status:        m.Status,
		Incarnation:   m.Incarnation,
		LastHeartbeat: time.Now(),
		SuspectTimer:  time.Now(),
	}

	// If member doesn't exist, add it (but only if not dead)
	if !exists {
		if m.Status != Dead { // only add alive nodes
			ml.members[m.NodeID] = updated
			select {
			case n.JoinCh <- m.NodeID:
			default:
				log.Printf("JoinCh full, dropped dead notification for %s", m.NodeID)
			}	
			log.Printf("JOINED: %s\n", m.NodeID)
		}
		return
	}

	// Don't update if existing member is already dead
	if existing.Status == Dead {
		return
	}

	// Handle updates based on incarnation number (version control)
	if updated.Incarnation > existing.Incarnation {
		// Higher incarnation always wins (node resurrected)
		if updated.Status != ml.members[m.NodeID].Status && updated.Status == Dead {
			log.Printf("Higher incarnation: Updating status to %s: %s\n", updated.Status, updated.NodeID)
		}
		ml.members[m.NodeID] = updated
	} else if updated.Incarnation == existing.Incarnation {
		// Same incarnation: only allow certain status transitions
		if updated.Status == Dead || (updated.Status == Suspect && existing.Status == Alive) {
			if updated.Status != ml.members[m.NodeID].Status {
				log.Printf("Same incarnation: Updating status to %s: %s\n", updated.Status, updated.NodeID)
			}
			existing.Status = updated.Status
			existing.SuspectTimer = time.Now()
		}
		/*
		   // Update LastHeartbeat if newer
		   if updated.LastHeartbeat.After(existing.LastHeartbeat) {
		       existing.LastHeartbeat = updated.LastHeartbeat
		   }*/

	}
}

// MarkSuspect marks a node as suspected of failure in the membership list
// Only transitions from Alive to Suspect status
func (ml *MembershipList) MarkSuspect(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if m, ok := ml.members[nodeID]; ok && m.Status == Alive {
		m.Status = Suspect
		m.SuspectTimer = time.Now()
		//log.Printf("Updating status to %s: %s\n", m.Status, m.NodeID)
	}
}

// CleanupDead removes dead nodes from membership and transitions suspect nodes to dead
// timeout: how long to wait before marking suspect nodes as dead
// deathtimer: how long to keep dead nodes before removing them from membership
func (ml *MembershipList) CleanupDead(node *Node, timeout time.Duration, deathtimer time.Duration) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	now := time.Now()

	for _, m := range ml.members {

		if m.Status == Dead && now.Sub(m.SuspectTimer) > deathtimer {
			//log.Printf("Deleting node: %s", m.NodeID)
			select {
			case node.FailCh <- m.NodeID:
			default:
				log.Printf("FailCh full, dropped dead notification for %s", m.NodeID)
			}		
			delete(ml.members, m.NodeID)
			continue
		}

		// Transition suspect nodes to dead after timeout period
		if m.Status == Suspect && now.Sub(m.SuspectTimer) > timeout {
			m.Status = Dead
			m.SuspectTimer = now
			log.Printf("Updating status to %s: %s\n", m.Status, m.NodeID)	
		}

	}
}

type Node struct {
	ID               string          // unique identity = AddrPort + TimeCreated
	Addr             string          // just ip:port
	Membership       *MembershipList // local view of cluster membership
	Heartbeat        int             // current heartbeat counter (incarnation number)
	GossipInterval   time.Duration   // interval between gossip rounds
	SuspicionTimeout time.Duration   // timeout before marking suspect nodes as dead
	FailureTimeout   time.Duration   // timeout before marking nodes as suspect
	tCleanup         time.Duration   // cleanup timer for removing dead nodes

	FailCh chan string // channel for receiving failures
	JoinCh chan string
}

// NewNode creates a new node instance with default configuration
// Generates a unique node ID based on address and timestamp
func NewNode(Addr string) *Node {
	start := time.Now()
	nodeID := fmt.Sprintf("%s:9000-%d", Addr, start.Unix())

	node := &Node{
		ID:               nodeID,
		Addr:             Addr,
		Membership:       NewMembershipList(),
		Heartbeat:        0,
		GossipInterval:   300 * time.Millisecond,
		SuspicionTimeout: 1700 * time.Millisecond,
		FailureTimeout:   1200 * time.Millisecond,
		tCleanup:         1000 * time.Millisecond,
		FailCh:           make(chan string, 100), // initialize channel
		JoinCh:           make(chan string, 100), // initialize channel
	}

	// Add self to membership as the first alive member
	node.AddOrUpdate(&Member{
		NodeID:        nodeID,
		Addr:          Addr,
		Status:        Alive,
		Incarnation:   0,
		LastHeartbeat: time.Now(),
		SuspectTimer:  time.Time{},
	})

	return node

}

// Listen receives gossip messages from other nodes on UDP port 9000
// Processes incoming membership updates and applies them to local membership list
func (n *Node) Listen() {
	addr, _ := net.ResolveUDPAddr("udp", n.Addr+":9000")
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buf := make([]byte, 8192)
	for {
		nBytes, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		// Parse incoming membership data
		var incoming map[string]*Member
		if err := json.Unmarshal(buf[:nBytes], &incoming); err != nil {
			continue
		}

		// Update local membership with incoming data
		for _, m := range incoming {
			n.AddOrUpdate(m)
		}
	}
}

// Gossip sends the local membership list to randomly selected peers
// Implements the gossip protocol by periodically exchanging membership information
func (n *Node) Gossip() {
	n.Membership.mu.Lock()
	peers := []string{}
	for _, m := range n.Membership.members {
		if m.Status != Dead && m.Addr != n.Addr {
			peers = append(peers, m.Addr)
		}
	}


	snapshot := make(map[string]*Member, len(n.Membership.members))
    for k, v := range n.Membership.members {
        copyMember := *v
        snapshot[k] = &copyMember
    }

    // Unlock ASAP to avoid blocking writers
    n.Membership.mu.Unlock()

	if len(peers) < 1 {
		return // no one to gossip to
	}

	// pick one random peer
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	peer := peers[0]

	var peer2 string
	if len(peers) > 1 {
		peer2 = peers[1]
	} else {
		peer2 = "" // or skip sending to peer2
	}

	conn, err := net.Dial("udp", peer+":9000")
	if err != nil {
		return
	}
	defer conn.Close()

	msg, _ := json.Marshal(snapshot)
	// Only attempt second peer if it exists
	if peer2 != "" {
		conn1, err1 := net.Dial("udp", peer2+":9000")
		if err1 == nil {
			defer conn1.Close()
			conn1.Write(msg)
		}
	}

	// Always write to first peer
	conn.Write(msg)
}

// IncrementHeartbeat increases the heartbeat counter and updates self in membership
// Used in gossip mode to periodically announce node liveness
func (n *Node) IncrementHeartbeat() {
	n.Heartbeat++
	m := &Member{
		NodeID:        n.ID,
		Addr:          n.Addr,
		Status:        Alive,
		Incarnation:   n.Heartbeat,
		LastHeartbeat: time.Now(),
		SuspectTimer:  time.Time{},
	}
	n.AddOrUpdate(m)
}

// CheckSuspicion monitors nodes for failure detection
// Marks nodes as suspect based on heartbeat timeout (always uses suspicion mode)
func (n *Node) CheckSuspicion() {
    now := time.Now()

    n.Membership.mu.Lock()
    //Make a copy of members to safely iterate
    snapshot := make([]*Member, 0, len(n.Membership.members))
    for _, m := range n.Membership.members {
        snapshot = append(snapshot, m)
    }
    n.Membership.mu.Unlock()

    for _, m := range snapshot {
        if m.Status == Alive && now.Sub(m.LastHeartbeat) > n.FailureTimeout {
            // MarkSuspect likely modifies the map — so take the write lock there
            n.Membership.MarkSuspect(m.NodeID)
        }
    }
}


// Run starts the main node operation loop
// Launches listener goroutines and executes gossip protocol
func (n *Node) Run() {
	// Start background listeners for both gossip and SWIM protocols
	go n.Listen()
	// Create ticker for periodic gossip protocol execution
	ticker := time.NewTicker(n.GossipInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Always run in gossip mode with suspicion enabled
		n.IncrementHeartbeat()
		n.Gossip()
		n.CheckSuspicion()
		n.Membership.CleanupDead(n, n.SuspicionTimeout, n.tCleanup)
	}
}

// printMembership displays the current membership list to console
func (n *Node) printMembership() {
	fmt.Println("Membership List:")
	for _, m := range n.Membership.members {
		fmt.Printf("  %s | %s \n", m.NodeID, m.Status)
	}
}

// printSuspected displays all nodes currently marked as suspect
func (n *Node) printSuspected() {
	fmt.Println("Suspected nodes:")
	for _, m := range n.Membership.members {
		if m.Status == Suspect {
			fmt.Println("  ", m.Addr)
		}
	}
}

// LeaveGroup handles node departure from the cluster
// Currently exits the process immediately
func (n *Node) LeaveGroup() {
	// mark self as "left" in membership, maybe broadcast
	os.Exit(0)
}

// StdinListener handles interactive commands from standard input
// Supports both single-word commands and mode switching commands
func (n *Node) StdinListener() {
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())

			// Handle single-word commands
			switch line {
			case "list_mem":
				n.printMembership()
				continue
			case "list_self":
				fmt.Println("Self ID:", n.ID)
				continue
			case "leave":
				log.Printf("Leaving group: %s", n.ID)
				n.LeaveGroup()
			case "display_suspects":
				n.printSuspected()
				continue
			default:
				// Mode switching disabled - always stays in gossip mode with suspicion enabled
				fmt.Println("Invalid input. Available commands: list_mem, list_self, leave, display_suspects, display_protocol")
				fmt.Println("Mode switching is disabled. Program always runs in gossip mode with suspicion enabled.")
			}
		}
	}()
}

func (n *Node) Responder(line string) bool {
	switch line {
	case "list_mem":
		n.printMembership()
		return true
	case "list_self":
		fmt.Println("Self ID:", n.ID)
		return true
	case "leave":
		log.Printf("Leaving group: %s", n.ID)
		n.LeaveGroup()
	case "display_suspects":
		n.printSuspected()
		return true
	default:
		//fmt.Println("Invalid input. Available commands: list_mem, list_self, leave, display_suspects, display_protocol")
		//fmt.Println("Mode switching is disabled. Program always runs in gossip mode with suspicion enabled.")
		return false
	}
	return false
}

// bootstrap connects to existing cluster members to join the network
// Attempts to connect to each VM in the list and exchange membership information
func bootstrap(node *Node, vms []string, self string) {
	for _, vm := range vms {
		if vm == self+":9000" {
			continue // skip self
		}

		conn, err := net.Dial("tcp", vm) // TCP for bootstrap
		if err != nil {
			fmt.Println("Could not connect to", vm, err)
			continue
		}

		// Send our current membership list to the peer
		msg, _ := json.Marshal(node.Membership.members)
		conn.Write(msg)

		// Receive their membership list
		buf := make([]byte, 8192)
		nBytes, err := conn.Read(buf)
		if err == nil {
			var incoming map[string]*Member
			if err := json.Unmarshal(buf[:nBytes], &incoming); err == nil {
				for _, m := range incoming {
					node.AddOrUpdate(m)
				}
			}
		}

		conn.Close()
		fmt.Println("Bootstrapped successfully from", vm)
		return // Successfully joined cluster, exit bootstrap process
	}

	fmt.Println("could not bootstrap from any known VM")
}

// startBootstrapListener starts a TCP listener to accept bootstrap connections
// Allows new nodes to join the cluster by exchanging membership information
func startBootstrapListener(node *Node, addr string) {
	go func() {
		ln, err := net.Listen("tcp", addr+":9000")
		if err != nil {
			panic(err)
		}
		defer ln.Close()

		fmt.Println("Listening for bootstrap connections on", addr+":9000")

		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}

			go func(c net.Conn) {
				defer c.Close()

				// Receive their membership list
				buf := make([]byte, 8192)
				nBytes, err := c.Read(buf)
				if err != nil {
					return
				}

				// Merge their membership data into our view
				var incoming map[string]*Member
				if err := json.Unmarshal(buf[:nBytes], &incoming); err == nil {
					for _, m := range incoming {
						node.AddOrUpdate(m)
					}
				}

				// Reply with our membership list
				msg, _ := json.Marshal(node.Membership.members)
				c.Write(msg)
			}(conn)
		}
	}()
}

func getLocalIP() string {
	// Get the first non-loopback IPv4 address
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal("Error getting addresses:", err)
	}

	for _, addr := range addrs {
		// Check if it's an IPv4 address and not a loopback
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil && !ipnet.IP.IsLoopback() {
			return ipnet.IP.String()
		}
	}
	return ""
}

// main is the entry point for the  application
// Parses command line arguments, initializes the node, and starts all services
func InitializeFailureDetection() *Node {
	// Parse command line arguments
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
/*
	// Find the index of this node in the VM list for logging
	idx := -1
	for i, v := range vms {
		if v == (string(selfIP) + ":9000") {
			idx = i + 1
			break
		}
	}
*/
	// Set up logging to both console and file
	//filename := fmt.Sprintf("machine.%02d.log", idx)

	//f, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer f.Close()

	// Create multi-writer to log to both stdout and file
	//mw := io.MultiWriter(os.Stdout, f)
	//log.SetOutput(f)

	// Create and configure the node
	node := NewNode(string(selfIP))

	// Attempt to bootstrap from existing cluster members
	bootstrap(node, vms, string(selfIP))
	// Start listening for new nodes wanting to join
	startBootstrapListener(node, string(selfIP))

	// Start interactive command listener
	//node.StdinListener()

	// Initialize random number generator
	rand.Seed(time.Now().UnixNano())
	// Start the main node operation (this blocks forever)
	go node.Run()

	return node
}



//NEW MP3 STUFF
func (ml *MembershipList) NodeIDs() []string {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ids := make([]string, 0, len(ml.members))
	for id, m := range ml.members {
		if m.Status != Dead { // ✅ skip dead nodes
			ids = append(ids, id)
		}
	}
	return ids

}
