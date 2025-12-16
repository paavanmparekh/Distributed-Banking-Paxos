// Node Life Management
// Includes activities performed by Node in the system
// 1. Node Initialization
// 2. Start Election
// 3. Start NEW-VIEW
package main

import (
	configurations "cft-paavanmparekh/Project1/Configurations"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

func BroadcastNewViewRPC(logger *Logger, Node *NodeService, BallotNumber configurations.BallotNumber, AcceptLog []configurations.AcceptLog) {
	var mu sync.Mutex
	var wg sync.WaitGroup

	CountAcceptedMsgs := make(map[int]int)
	SeqToTxnMap := make(map[int]configurations.AcceptTxn)
	for i := 1; i <= 5; i++ {
		if i == BallotNumber.NodeID {
			continue
		}
		var NewViewInput configurations.NewViewInput
		NewViewInput.B = BallotNumber
		NewViewInput.AcceptLog = AcceptLog
		//Send Accept msg with your own ballot number
		for i := range NewViewInput.AcceptLog {
			NewViewInput.AcceptLog[i].AcceptNum = BallotNumber
		}
		wg.Add(1)
		go func(targetNode int) {
			defer wg.Done()
			port := configurations.GetNodePort(targetNode)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				logger.Log("[Node %v] Failed to connect to node %d: %v\n", BallotNumber.NodeID, targetNode, err)
				return
			}
			defer client.Close()

			var reply map[int]configurations.AcceptTxn
			err = client.Call("NodeService.NewView", NewViewInput, &reply)
			if err != nil {
				logger.Log("[Node %v] NewView RPC to node %d failed: %v\n", BallotNumber.NodeID, targetNode, err)
			} else {
				mu.Lock()
				for key, value := range reply {
					CountAcceptedMsgs[key] += value.Acceptance
					SeqToTxnMap[key] = value
				}
				mu.Unlock()
			}
		}(i)

	}
	wg.Wait()
	logger.Log("[Node %v] Received Accept Msgs from all nodes\n", CountAcceptedMsgs)
	Node.isNewView = false //Don't wait for the AcceptLog msgs to commit
	for key, val := range CountAcceptedMsgs {
		if val+1 >= 3 {
			logger.Log("[Node %v] Majority Accepted Txn with Sequence No: %v, commiting it now...\n", BallotNumber.NodeID, key)

			BroadcastCommitRPC(Node, SeqToTxnMap[key])
		} else {
			logger.Log("[Node %v] Txn with Sequence No %v Not Accepted by Majority\n", BallotNumber.NodeID, key)
		}
	}
}

func BroadcastPrepareRPC(logger *Logger, Node *NodeService, PB configurations.BallotNumber) {

	votes := 0
	acceptMap := make(map[int]struct {
		Ballot configurations.BallotNumber
		Txn    configurations.Transaction
	})

	var mu sync.Mutex
	var wg sync.WaitGroup
	logger.Log("[Node %v] Sending Prepare RPC to all nodes\n", PB.NodeID)
	for i := 1; i <= 5; i++ {
		if i == PB.NodeID {
			continue
		}

		wg.Add(1)
		go func(targetNode int) {
			defer wg.Done()
			port := configurations.GetNodePort(targetNode)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				logger.Log("[Node %v] Failed to connect to node %d: %v\n", PB.NodeID, targetNode, err)
				return
			}
			defer client.Close()

			var reply configurations.Promise
			err = client.Call("NodeService.Prepare", PB, &reply)
			if err != nil {
				logger.Log("[Node %v] Prepare RPC to node %d failed: %v\n", PB.NodeID, targetNode, err)
			} else {
				mu.Lock()
				votes += reply.Vote
				if reply.Vote == 1 && len(reply.AcceptedMsgs) != 0 {
					ReceivedAcceptLog := reply.AcceptedMsgs
					for _, msg := range ReceivedAcceptLog {

						if entry, exists := acceptMap[msg.AcceptSeq]; !exists {
							acceptMap[msg.AcceptSeq] = struct {
								Ballot configurations.BallotNumber
								Txn    configurations.Transaction
							}{msg.AcceptNum, msg.AcceptVal}
						} else if msg.AcceptNum.B >= entry.Ballot.B {
							acceptMap[msg.AcceptSeq] = struct {
								Ballot configurations.BallotNumber
								Txn    configurations.Transaction
							}{msg.AcceptNum, msg.AcceptVal}
						}
					}
				}
				mu.Unlock()
			}
		}(i)

	}
	wg.Wait()
	var AcceptLog []configurations.AcceptLog
	maxSequenceNum := -1
	for k := range acceptMap {
		if k > maxSequenceNum {
			maxSequenceNum = k
		}
	}
	logger.Log("[Node %v] Received %v votes\n", PB.NodeID, votes)
	logger.Log("[Node %v] Received AcceptLogs From each Node: %v\n", PB.NodeID, acceptMap)

	if votes+1 >= 3 {
		logger.Log("[Node %v] Elected as Leader, Starting NEW-VIEW if AcceptLog Exists\n", PB.NodeID)
		if maxSequenceNum == -1 {
			logger.Log("[Node %v] No AcceptLog exists\n", PB.NodeID)
		}

		// Ensure the new leader's local sequence number is at least the highest seen
		if maxSequenceNum >= 0 {
			Node.mu.Lock()
			if Node.SequenceNumber < maxSequenceNum {
				Node.SequenceNumber = maxSequenceNum
				logger.Log("[Node %v] UPDATED SequenceNumber to %d after election (from Prepare)\n", PB.NodeID, Node.SequenceNumber)
			}
			Node.mu.Unlock()
		}
		for i := 1; maxSequenceNum >= 1 && i <= maxSequenceNum; i++ {
			if _, ok := acceptMap[i]; !ok {
				//For "no-op"
				AcceptLog = append(AcceptLog, configurations.AcceptLog{AcceptSeq: i, AcceptNum: configurations.BallotNumber{}, AcceptVal: configurations.Transaction{}, Status: "no-op"})
				continue
			}
			AcceptLog = append(AcceptLog, configurations.AcceptLog{AcceptSeq: i, AcceptNum: acceptMap[i].Ballot, AcceptVal: acceptMap[i].Txn, Status: "regular"})
		}
		if len(AcceptLog) != 0 {
			Node.isNewView = true
			logger.Log("[Node %v] Broadcasting NEW-VIEW RPCs %v\n\n", PB.NodeID, AcceptLog)

			BroadcastNewViewRPC(logger, Node, PB, AcceptLog)
		}
	} else {
		logger.Log("[Node %v] Lost Election\n", PB.NodeID)
	}

}

func main() {
	nodeID := flag.Int("id", 1, "Node ID (1-5)")
	flag.Parse()

	if *nodeID < 1 || *nodeID > 5 {
		log.Fatalf("Invalid node ID: %d. Must be between 1 and 5", *nodeID)
	}

	port := configurations.GetNodePort(*nodeID)
	if port == 0 {
		log.Fatalf("No port found for node ID %d", *nodeID)
	}

	election_timeout := configurations.GetNodeElectionTimeout(*nodeID)
	tp_timeout := configurations.GetNodeProposalTimeout(*nodeID)
	db := configurations.InitializeDBWithNodeID(*nodeID)
	api := &NodeService{
		NodeConfig:             configurations.NodeConfig{ID: *nodeID, Bnum: configurations.BallotNumber{B: 0, NodeID: *nodeID}, Port: port, T: election_timeout, Tp: tp_timeout, IsLive: true, DB: db, TransactionStatus: make(map[int]string)},
		StartupPhase:           true,
		executionResults:       make(map[int]configurations.TxnReply),
		PendingClientResponses: make(map[int]chan configurations.TxnReply),
	}
	api.ElectionTimer = time.NewTimer(election_timeout)
	api.TpTimer = time.NewTimer(tp_timeout)
	srv := rpc.NewServer()
	err := srv.Register(api)
	if err != nil {
		log.Fatalf("error registering API for node %d: %v", *nodeID, err)
	}

	h := http.NewServeMux()
	h.Handle("/", srv)

	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listener error for node %d: %v", *nodeID, err)
	}

	log.Printf("Node %d serving RPC on port %d", *nodeID, port)

	go http.Serve(listener, h)
	logger := GetLogger(*nodeID)

	go func() {
		for {
			<-api.ElectionTimer.C

			// During startup: always allow elections
			// After startup: only if IsLive is true
			if !api.StartupPhase && !api.IsLive {
				api.ElectionTimer.Reset(election_timeout)
				continue
			}

			var PB configurations.BallotNumber
			PB.B = api.Bnum.B + 1
			PB.NodeID = *nodeID
			logger.Log("[Node %d] Election Timeout, starting election with ballot %d\n", PB.NodeID, PB.B)
			api.Bnum = PB //needed
			select {
			case <-api.TpTimer.C:
				BroadcastPrepareRPC(logger, api, PB)
				api.TpTimer.Reset(tp_timeout)
			default:
				logger.Log("[Node %d] Skip Election, Wait for Tp Timeout\n", *nodeID)
			}
			api.ElectionTimer.Reset(election_timeout)
		}
	}()

	// Interactive menu
	for {
		var choice int
		fmt.Println("1: Print Log")
		fmt.Println("2: Print DB")
		fmt.Println("3: Print Status")
		fmt.Println("4: Print View")
		fmt.Println("5: Clear Terminal")
		fmt.Println("6: Exit")
		fmt.Printf("\nSelect an option: ")
		fmt.Scan(&choice)

		switch choice {
		case 1:
			logger.PrintLogContent()
		case 2:
			api.DB.PrintDB()
		case 3:
			var seq int
			fmt.Printf("Enter Sequence No: ")
			fmt.Scan(&seq)
			fmt.Printf("Transaction Status: %v\n", api.GetTransactionStatus(seq))
		case 4:
			fmt.Printf("NEW-VIEW: \n")

			for _, entry := range api.AcceptLog {
				fmt.Printf("Seq: %d, Ballot: %v, Transaction: %v, Status: %v\n",
					entry.AcceptSeq, entry.AcceptNum, entry.AcceptVal, entry.Status)
			}
		case 5:
			fmt.Print("\033[H\033[2J")
		case 6:
			api.DB.Close()
			return
		default:
			fmt.Println("Invalid choice")
		}
	}

}
