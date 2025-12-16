// RPC Calls in the system with helper functions:
// 1. ClientRequest
// 2. Accept
// 3. Prepare
// 4. NewView
// 5. Commit
// 6. FailNode
package main

import (
	configurations "cft-paavanmparekh/Project1/Configurations"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

type NodeService struct {
	configurations.NodeConfig
	ElectionTimer          *time.Timer
	TpTimer                *time.Timer
	isNewView              bool
	StartupPhase           bool
	mu                     sync.RWMutex
	executionResults       map[int]configurations.TxnReply
	resultsMutex           sync.RWMutex
	PendingClientResponses map[int]chan configurations.TxnReply
	pendingMutex           sync.RWMutex
}

func notifyPendingClient(n *NodeService, seqNo int, result configurations.TxnReply) {
	n.pendingMutex.Lock()
	defer n.pendingMutex.Unlock()
	if ch, exists := n.PendingClientResponses[seqNo]; exists {
		select {
		case ch <- result:
		default:
		}
		delete(n.PendingClientResponses, seqNo)
	}
}

func ExecuteTxn(logger *Logger, n *NodeService, txn configurations.Transaction, status string) bool {
	logger.Log("[Node %d] Executing transaction: (%v %v %v) \n", n.ID, txn.Sender, txn.Receiver, txn.Amount)
	if status == "no-op" {
		logger.Log("[Node %d] NO-OP: Not changing the state\n", n.ID)
		return true
	}
	var result bool
	if txn.Amount != 0 {
		n.DB.UpdateEntry(txn, &result)
		logger.Log("[Node %d] Transaction (%v %v %v) executed successfully: %v\n", n.ID, txn.Sender, txn.Receiver, txn.Amount, result)
	}
	return result
}

func BroadcastCommitRPC(n *NodeService, AcceptObj configurations.AcceptTxn) {
	logger := GetLogger(n.ID)
	logger.Log("[Node %d] LEADER: Starting commit phase for SeqNo %d\n", n.ID, AcceptObj.SeqNo)

	// Mark as committed (will be executed locally or already executed)
	if _, ok := n.TransactionStatus[AcceptObj.SeqNo]; !ok {
		n.TransactionStatus[AcceptObj.SeqNo] = "Commmited"
	}
	for i := 1; i <= 5; i++ {
		if i == n.ID {
			continue
		}

		go func(targetNode int) {

			port := configurations.GetNodePort(targetNode)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				logger.Log("Failed to connect to node %d: %v\n", targetNode, err)
				return
			}
			defer client.Close()

			var reply configurations.TxnReply //No use of these replies since commit msgs to all nodes don't have acks
			err = client.Call("NodeService.Commit", AcceptObj, &reply)
			if err != nil {
				logger.Log("[Node %d] COMMIT RPC to Node %d failed: %v\n", n.ID, targetNode, err)
			} else {
				logger.Log("[Node %d] COMMIT RPC to Node %d successful\n", n.ID, targetNode)
			}
		}(i)

	}
	//return txn_res
}

func BroadcastAcceptRPC(n *NodeService, txn configurations.Transaction) *configurations.TxnReply {
	logger := GetLogger(n.ID)
	acceptance := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	txn_reply := configurations.TxnReply{}
	n.mu.Lock()
	var AccpetObj configurations.AcceptTxn
	AccpetObj.Txn = txn
	AccpetObj.B = n.Bnum

	// Safety: ensure we don't assign a sequence number lower than last executed
	if n.SequenceNumber < n.LastExecuted {
		n.SequenceNumber = n.LastExecuted
	}
	n.SequenceNumber += 1
	AccpetObj.SeqNo = n.SequenceNumber

	logger.Log("[Node %d] LEADER: Assigned SeqNo %d (LastExecuted=%d)\n", n.ID, AccpetObj.SeqNo, n.LastExecuted)

	//Accepting txn by own (leader)
	n.AcceptLog = append(n.AcceptLog, configurations.AcceptLog{AcceptNum: AccpetObj.B, AcceptSeq: AccpetObj.SeqNo, AcceptVal: AccpetObj.Txn})
	if _, ok := n.TransactionStatus[AccpetObj.SeqNo]; !ok {
		n.TransactionStatus[AccpetObj.SeqNo] = "Accepted"
	}
	n.mu.Unlock()

	logger.Log("[Node %d] LEADER: Starting ACCEPT phase consensus (Ballot: %v, (%v %v %v) SeqNo: %d)\n", n.ID, n.Bnum, txn.Sender, txn.Receiver, txn.Amount, AccpetObj.SeqNo)
	for i := 1; i <= 5; i++ {
		if i == n.ID {
			continue
		}

		wg.Add(1)
		go func(targetNode int) {
			defer wg.Done()
			port := configurations.GetNodePort(targetNode)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				logger.Log("Failed to connect to node %d: %v\n", targetNode, err)
				return
			}
			defer client.Close()

			var reply configurations.AcceptTxn
			err = client.Call("NodeService.Accept", AccpetObj, &reply)
			if err != nil {
				logger.Log("[Node %d] ACCEPT RPC to Node %d failed: %v\n", n.ID, targetNode, err)
			} else {
				logger.Log("[Node %d] ACCEPT RPC to Node %d returned: %d\n", n.ID, targetNode, reply.Acceptance)
				mu.Lock()
				acceptance += reply.Acceptance
				mu.Unlock()
			}
		}(i)

	}

	wg.Wait()
	logger.Log("[Node %d] ACCEPT phase complete: %d acceptances (need 3 for majority)\n", n.ID, acceptance+1)
	if acceptance+1 >= 3 {
		logger.Log("[Node %d] MAJORITY ACHIEVED: Proceeding to COMMIT phase\n", n.ID)
		if _, ok := n.TransactionStatus[AccpetObj.SeqNo]; !ok {
			n.TransactionStatus[AccpetObj.SeqNo] = "Commmited"
		}
		var txn_reply configurations.TxnReply
		var wg sync.WaitGroup
		if n.LastExecuted < AccpetObj.SeqNo {
			wg.Add(1)
			go func() {
				ExecuteSerially(n, AccpetObj, &txn_reply)
				wg.Done()
			}()
			wg.Wait()
		}
		// Use stored execution result instead of re-executing
		n.resultsMutex.RLock()
		if storedResult, exists := n.executionResults[AccpetObj.SeqNo]; exists {
			txn_reply = storedResult
		}
		n.resultsMutex.RUnlock()

		// If gap found, wait for actual execution
		if txn_reply.Msg == "Gap Found" {
			logger.Log("[Node %d] LEADER: Gap detected, waiting for actual execution of SeqNo %d\n", n.ID, AccpetObj.SeqNo)
			// Create channel for this sequence number
			responseCh := make(chan configurations.TxnReply, 1)
			n.pendingMutex.Lock()
			if n.PendingClientResponses == nil {
				n.PendingClientResponses = make(map[int]chan configurations.TxnReply)
			}
			n.PendingClientResponses[AccpetObj.SeqNo] = responseCh
			n.pendingMutex.Unlock()

			// Wait for actual execution with timeout (shortened)
			select {
			case actualResult := <-responseCh:
				txn_reply = actualResult
				logger.Log("[Node %d] LEADER: Received actual execution result for SeqNo %d: (%v %v)\n", n.ID, AccpetObj.SeqNo, txn_reply.Res, txn_reply.Msg)
			case <-time.After(5 * time.Second):
				txn_reply.Msg = "Execution Timeout"
				logger.Log("[Node %d] LEADER: Timeout waiting for execution of SeqNo %d\n", n.ID, AccpetObj.SeqNo)
				n.pendingMutex.Lock()
				delete(n.PendingClientResponses, AccpetObj.SeqNo)
				n.pendingMutex.Unlock()
			}
		}

		logger.Log("[Node %d] LEADER: Executed locally (Ballot: %v, SeqNo: %d, (%v %v %v)) Result: (%v %v)\n", n.ID, n.Bnum, AccpetObj.SeqNo, AccpetObj.Txn.Sender, AccpetObj.Txn.Receiver, AccpetObj.Txn.Amount, txn_reply.Res, txn_reply.Msg)
		BroadcastCommitRPC(n, AccpetObj)
		return &txn_reply
	}

	logger.Log("[Node %d] MAJORITY NOT ACHIEVED: Triggering new leader election\n", n.ID)
	n.ElectionTimer.Reset(0) //System undergoes leader election again.
	txn_reply.Msg = "Majority Not Accepted"
	return &txn_reply
}

func ExecuteSerially(n *NodeService, AcceptObj configurations.AcceptTxn, reply *configurations.TxnReply) {
	logger := GetLogger(n.ID)
	logger.Log("[Node %d] COMMIT: Received commit for SeqNo %d, Ballot %v\n", n.ID, AcceptObj.SeqNo, AcceptObj.B)
	n.TransactionStatus[AcceptObj.SeqNo] = "Commited"

	n.mu.Lock()
	if n.PendingCommands == nil {
		n.PendingCommands = make(map[int]configurations.AcceptTxn)
	}
	n.PendingCommands[AcceptObj.SeqNo] = AcceptObj
	logger.Log("[Node %d] COMMIT: Stored command at SeqNo %d in pending queue\n", n.ID, AcceptObj.SeqNo)
	n.mu.Unlock()

	for {
		n.mu.Lock()
		nextSeq := n.LastExecuted + 1
		if cmd, exists := n.PendingCommands[nextSeq]; exists {
			// Execute the next sequential command
			logger.Log("[Node %d] COMMIT: Executing sequential command at SeqNo %d\n", n.ID, nextSeq)
			reply.Res = ExecuteTxn(logger, n, cmd.Txn, cmd.Status)
			if reply.Res {
				reply.Msg = "Successful"
			} else {
				reply.Msg = "Failed"
			}
			// Store execution result
			n.resultsMutex.Lock()
			if n.executionResults == nil {
				n.executionResults = make(map[int]configurations.TxnReply)
			}
			n.executionResults[nextSeq] = *reply
			n.resultsMutex.Unlock()
			n.TransactionStatus[cmd.SeqNo] = "Executed"
			delete(n.PendingCommands, nextSeq)
			n.LastExecuted = nextSeq
			logger.Log("[Node %d] COMMIT: Executed command at SeqNo %d, Result: (%v %v)\n", n.ID, nextSeq, reply.Res, reply.Msg)
			n.mu.Unlock()
			// Notify any pending client for this sequence number
			notifyPendingClient(n, nextSeq, *reply)
		} else if len(n.PendingCommands) != 0 {
			// Gap found, stop execution until lower sequences arrive
			logger.Log("[Node %d] COMMIT: Gap detected at SeqNo %d, waiting for missing commands\n", n.ID, nextSeq)
			reply.Msg = "Gap Found"
			n.mu.Unlock()
			break
		} else {
			// No more commands to execute
			logger.Log("[Node %d] COMMIT: All commands executed\n", n.ID)
			n.mu.Unlock()
			break
		}
	}
}

func (n *NodeService) Commit(AcceptObj configurations.AcceptTxn, reply *configurations.TxnReply) error {
	if !n.IsLive {
		return nil
	}
	n.ElectionTimer.Reset(n.T)
	logger := GetLogger(n.ID)
	if n.LastExecuted < AcceptObj.SeqNo { //execute the command only if it is not already executed
		ExecuteSerially(n, AcceptObj, reply)
	}
	logger.Log("[Node %d] COMMIT: Txn (%v %v %v) with SeqNo %d already committed\n", n.ID, AcceptObj.Txn.Sender, AcceptObj.Txn.Receiver, AcceptObj.Txn.Amount, AcceptObj.SeqNo)
	return nil
}

func (n *NodeService) ClientRequest(txn configurations.Transaction, reply *configurations.Reply) error {
	if !n.IsLive {
		reply.Msg = "Node Not Live"
		return nil
	}
	logger := GetLogger(n.ID)

	n.mu.Lock()
	if n.ClientLastReply == nil {
		n.ClientLastReply = make(map[string]int)
	}
	T, ok := n.ClientLastReply[txn.Sender]
	if ok && T >= txn.Timestamp && n.TxnsProcessed[txn].Msg != "Majority Not Accepted" {
		*reply = configurations.Reply{Msg: "Invalid Transaction Timestamp"}
		n.mu.Unlock()
		return nil
	}
	if n.TxnsProcessed == nil {
		n.TxnsProcessed = make(map[configurations.Transaction]configurations.Reply)
	}
	val, ok := n.TxnsProcessed[txn]
	if ok && val.Msg != "Majority Not Accepted" {
		*reply = val
		reply.Msg = "Already Processed"
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	logger.Log("[Node %d] CLIENT REQUEST: Received from %s (Timestamp: %d), Current leader: Node %d\n", n.ID, txn.Sender, txn.Timestamp, n.Bnum.NodeID)
	if n.Bnum.NodeID != 0 && n.ID != n.Bnum.NodeID { //I am not the leader, let's redirect
		logger.Log("[Node %d] REDIRECT: Forwarding client request to leader Node %d\n", n.ID, n.Bnum.NodeID)
		leader_port := configurations.GetNodePort(n.Bnum.NodeID)
		addr := fmt.Sprintf("localhost:%d", leader_port)
		client, err := rpc.DialHTTP("tcp", addr)
		if err == nil {
			client.Call("NodeService.ClientRequest", txn, reply)
			client.Close()
			logger.Log("[Node %d] REDIRECT: Successfully forwarded to leader\n", n.ID)
			return nil
		}
		logger.Log("[Node %d] REDIRECT FAILED: Cannot reach leader Node %d, processing locally\n", n.ID, n.Bnum.NodeID)
	}

	if !n.isNewView {
		logger.Log("[Node %d] LEADER: Processing client request as current leader\n", n.ID)
		if n.TxnsProcessed[txn].Msg == "Majority Not Accepted" {
			reply.Msg = "Majority Not Accepted"
			return nil
		}
		var txn_res = BroadcastAcceptRPC(n, txn)
		n.mu.Lock()
		reply.B.B = n.Bnum.B
		reply.B.NodeID = n.ID
		reply.Result = txn_res.Res
		reply.Timestamp = txn.Timestamp
		reply.Msg = txn_res.Msg
		n.ClientLastReply[txn.Sender] = txn.Timestamp
		n.TxnsProcessed[txn] = *reply

		logger.Log("[Node %d] CLIENT RESPONSE: Sending result %v to client %s\n", n.ID, txn_res, txn.Sender)
		n.mu.Unlock()
	} else {
		logger.Log("[Node %d] NEW VIEW: Cannot process client request during view change\n", n.ID)
	}
	return nil
}

func (n *NodeService) Accept(AccpetObj configurations.AcceptTxn, reply *configurations.AcceptTxn) error {
	if !n.IsLive {
		return nil
	}
	n.ElectionTimer.Reset(n.T)
	logger := GetLogger(n.ID)

	n.mu.Lock()
	defer n.mu.Unlock()

	logger.Log("[Node %d] ACCEPT RPC: Received from Node %d | (SeqNo: %d) | Ballot Comparision (%v) Vs (%v)\n", n.ID, AccpetObj.B.NodeID, AccpetObj.SeqNo, n.Bnum, AccpetObj.B)

	if n.Bnum.B <= AccpetObj.B.B {
		n.Bnum.B = AccpetObj.B.B
		n.Bnum.NodeID = AccpetObj.B.NodeID
		reply.Acceptance = 1
		reply.B = AccpetObj.B
		reply.SeqNo = AccpetObj.SeqNo
		reply.Txn = AccpetObj.Txn

		//entry in AcceptLog
		var AcceptEntry configurations.AcceptLog
		AcceptEntry.AcceptNum = AccpetObj.B
		AcceptEntry.AcceptSeq = AccpetObj.SeqNo
		AcceptEntry.AcceptVal = AccpetObj.Txn

		if n.SequenceNumber < AccpetObj.SeqNo {
			n.SequenceNumber = AccpetObj.SeqNo
		}
		n.AcceptLog = append(n.AcceptLog, AcceptEntry)
		logger.Log("[Node %d] ACCEPT: ACCEPTED proposal (Ballot: %v, SeqNo: %d)\n", n.ID, AccpetObj.B, AccpetObj.SeqNo)
		if _, ok := n.TransactionStatus[AccpetObj.SeqNo]; !ok {
			n.TransactionStatus[AccpetObj.SeqNo] = "Accepted"
		}
	} else {
		reply.Acceptance = 0
		logger.Log("[Node %d] ACCEPT: REJECTED proposal (Ballot too low: %v)\n", n.ID, AccpetObj.B)
	}

	return nil
}

func (n *NodeService) Prepare(ballotNum configurations.BallotNumber, Promise *configurations.Promise) error {
	if !n.IsLive {
		return nil
	}
	n.TpTimer.Reset(n.Tp)
	logger := GetLogger(n.ID)

	n.mu.Lock()
	defer n.mu.Unlock()

	logger.Log("[Node %d] PREPARE RPC: Received from Node %d | Ballot Comparision (%v) Vs (%v)\n", n.ID, ballotNum.NodeID, n.Bnum, ballotNum)

	if n.Bnum.B < ballotNum.B {
		n.Bnum.B = ballotNum.B
		n.Bnum.NodeID = ballotNum.NodeID
		Promise.AcceptedMsgs = n.AcceptLog
		Promise.Vote = 1
		logger.Log("[Node %d] PREPARE: PROMISED to Node %d (Ballot: %v, AcceptLog entries: %d)\n", n.ID, ballotNum.NodeID, ballotNum, len(n.AcceptLog))
	} else {
		Promise.AcceptedMsgs = nil
		Promise.Vote = 0
		logger.Log("[Node %d] PREPARE: REJECTED Node %d (Ballot too low: %v)\n", n.ID, ballotNum.NodeID, ballotNum)
	}

	return nil
}

func (n *NodeService) NewView(newViewObj configurations.NewViewInput, acceptanceMap *map[int]configurations.AcceptTxn) error {
	if !n.IsLive {
		return nil
	}
	n.ElectionTimer.Reset(n.T)

	n.mu.Lock()
	defer n.mu.Unlock()
	logger := GetLogger(n.ID)
	logger.Log("[Node %d] NEW VIEW: Processing new view from leader (AcceptLog entries: %d)\n", n.ID, len(newViewObj.AcceptLog))
	AcceptLog := newViewObj.AcceptLog
	// Track highest sequence number seen in this new view
	maxSeq := -1
	for _, entry := range AcceptLog {

		if n.Bnum.B <= entry.AcceptNum.B {
			n.Bnum.B = entry.AcceptNum.B
			n.Bnum.NodeID = entry.AcceptNum.NodeID

			//entry in AcceptLog
			var AcceptEntry configurations.AcceptLog
			AcceptEntry.AcceptNum = entry.AcceptNum
			AcceptEntry.AcceptSeq = entry.AcceptSeq
			AcceptEntry.AcceptVal = entry.AcceptVal
			n.AcceptLog = append(n.AcceptLog, AcceptEntry)
			logger.Log("[Node %d] NEW VIEW: Accepted entry SeqNo %d from Ballot %v\n", n.ID, entry.AcceptSeq, entry.AcceptNum)

			(*acceptanceMap)[entry.AcceptSeq] = configurations.AcceptTxn{B: AcceptEntry.AcceptNum, Txn: AcceptEntry.AcceptVal, SeqNo: AcceptEntry.AcceptSeq, Acceptance: 1, Status: AcceptEntry.Status}

			if entry.AcceptSeq > maxSeq {
				maxSeq = entry.AcceptSeq
			}

		} else {
			logger.Log("[Node %d] NEW VIEW: Rejected entry SeqNo %d (Ballot too low: %v Vs %v)\n", n.ID, entry.AcceptSeq, n.Bnum, entry.AcceptNum)
			(*acceptanceMap)[entry.AcceptSeq] = configurations.AcceptTxn{}
		}
	}

	// Ensure follower's local sequence number is at least maxSeq
	if maxSeq >= 0 && n.SequenceNumber < maxSeq {
		n.SequenceNumber = maxSeq
		logger.Log("[Node %d] UPDATED SequenceNumber to %d after applying NEW-VIEW\n", n.ID, n.SequenceNumber)
	}

	return nil
}

func (n *NodeService) FailNode(isLive bool, reply *bool) error {
	n.IsLive = isLive
	n.StartupPhase = false // Disable startup phase when main.go starts controlling nodes
	*reply = true
	logger := GetLogger(n.ID)
	if isLive {
		logger.Log("[Node %d] STATUS: Node is now LIVE and operational\n", n.ID)
	} else {
		logger.Log("[Node %d] STATUS: Node is now FAILED and non-operational\n", n.ID)
	}
	return nil
}
