// CSE 535 Distributed Systems
// Project 1: Multi-Paxos Protocol for Banking Application
// Author: Paavan Parekh
// Date: 11/10/2025
// Client Side Implimentation
package main

import (
	configurations "cft-paavanmparekh/Project1/Configurations"
	"encoding/csv"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OrderedItem struct {
	IsLF        bool
	Transaction configurations.Transaction
}

func processTransactionBatch(batch []configurations.Transaction, last_leader *int, timestamp map[string]int, timestampMutex *sync.RWMutex) {
	//Distinguish transactions from one sender
	//It will allow sequential transaction processing for signle client
	//And parallel/(concurrent) transaction processing for multiple clients
	var sender_transactions = make(map[string][]configurations.Transaction)
	for _, txn := range batch {
		sender_transactions[txn.Sender] = append(sender_transactions[txn.Sender], txn)
	}

	var wg sync.WaitGroup
	for i := 'A'; i <= 'J'; i++ {
		wg.Add(1)
		go func(Sender rune) {
			defer wg.Done()
			if _, ok := sender_transactions[string(Sender)]; !ok {
				return
			}
			client_requests := sender_transactions[string(Sender)]
			for _, txn := range client_requests {
				timestampMutex.RLock()
				txn.Timestamp = timestamp[txn.Sender]
				timestampMutex.RUnlock()
				txn_result := RPCCall_ClientRequest(&txn, *last_leader, configurations.GetClientTimeout(string(Sender)))
				if txn_result.B.NodeID != 0 {
					*last_leader = txn_result.B.NodeID //Last leader determined from the reply client receives
				} else {
					*last_leader = 1
				}
				timestampMutex.Lock()
				timestamp[txn.Sender] = txn.Timestamp + 1
				timestampMutex.Unlock()
				fmt.Printf("Transaction (%v %v %v, %v) Result: %+v\n", txn.Sender, txn.Receiver, txn.Amount, txn.Timestamp, txn_result)
			}
		}(i)
	}
	wg.Wait()
}

func triggerLeaderFailure(leader int) {
	fmt.Printf("LF: Failing current leader Node %d\n", leader)
	port := configurations.GetNodePort(leader)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err == nil {
		var reply bool
		client.Call("NodeService.FailNode", false, &reply)
		client.Close()
	}
	// Wait for new leader election (shortened to match reduced election timeouts)
	time.Sleep(4 * time.Second)
}

func restoreFailedLeader(leader int) {
	fmt.Printf("Restoring failed leader Node %d\n", leader)
	port := configurations.GetNodePort(leader)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err == nil {
		var reply bool
		client.Call("NodeService.FailNode", true, &reply)
		client.Close()
	}
	// Wait for system to stabilize after restoring a node
	time.Sleep(6 * time.Second)
}

func BroadcastClientRequest(txn *configurations.Transaction, leader int, request_timeout time.Duration) configurations.Reply {
	fmt.Printf("Broadcasting Client Request to other nodes from leader node %d\n", leader)
	for i := 1; i <= 5; i++ {
		if i != leader {
			port := configurations.GetNodePort(i)
			client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
			if err == nil {
				done := make(chan bool, 1)
				var reply configurations.Reply
				go func() {
					err := client.Call("NodeService.ClientRequest", txn, &reply)
					done <- (err == nil)
				}()
				select {
				case success := <-done:
					client.Close()
					if success && reply.Msg != "Node Not Live" {
						return reply
					}
				case <-time.After(request_timeout):
					client.Close()
					fmt.Printf("Broadcast to node %d timed out\n", i)
				}
			}
		}
	}
	return configurations.Reply{Msg: "Did't get reply from any node"}
}

func RPCCall_ClientRequest(txn *configurations.Transaction, leader int, request_timeout time.Duration) configurations.Reply {
	maxRetries := 5
	retries := 0

	for retries < maxRetries {
		port := configurations.GetNodePort(leader)
		fmt.Printf("Attempting to connect to node %d on port %d (attempt %d)\n", leader, port, retries+1)

		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			fmt.Printf("Failed to connect to node %d: %v\n", leader, err)
			txn.Timestamp++
			retries++
			time.Sleep(200 * time.Millisecond)
			continue
		}

		done := make(chan bool, 1)
		var reply configurations.Reply

		go func() {
			err := client.Call("NodeService.ClientRequest", txn, &reply)
			if err != nil {
				fmt.Printf("RPC call failed: %v\n", err)
			}
			done <- (err == nil)
		}()

		select {
		case success := <-done:
			if client != nil {
				client.Close()
			}
			if success {
				if reply.Msg == "Majority Not Accepted" {
					fmt.Printf("Retrying Client Request\n")

					retries++
					time.Sleep(200 * time.Millisecond)
					continue
				}
				if reply.Msg == "Node Not Live" {
					fmt.Printf("leader node %d not live\n", leader)
					broadcastReply := BroadcastClientRequest(txn, leader, request_timeout)
					fmt.Printf("Broadcast Reply when node was not live: %v\n", broadcastReply)
					if broadcastReply.Msg != "Did't get reply from any node" && broadcastReply.Msg != "Majority Not Accepted" {
						return broadcastReply
					}
					fmt.Printf("Retrying Client Request\n")

					retries++
					time.Sleep(400 * time.Millisecond)
					continue
				}
				return reply
			}
			retries++
		case <-time.After(request_timeout):
			fmt.Printf("Request to leader node %d timed out\n", leader)
			if client != nil {
				client.Close()
			}
			reply := BroadcastClientRequest(txn, leader, request_timeout)
			if reply.Msg != "Did't get reply from any node" && reply.Msg != "Majority Not Accepted" {
				return reply
			}
			fmt.Printf("Retrying Client Request\n")
			retries++
			time.Sleep(400 * time.Millisecond)
			continue
		}
	}

	fmt.Printf("Failed to complete request after %d retries\n", maxRetries)
	return configurations.Reply{Result: false, Msg: "Failed to complete request"}
}

func RPCCall_FailNodes(List_of_Livenodes []int) {
	for i := 1; i <= 5; i++ {
		status := false
		for _, node := range List_of_Livenodes {
			if i == node {
				status = true
				break
			}
		}
		port := configurations.GetNodePort(i)
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
		if err == nil {
			var reply bool
			client.Call("NodeService.FailNode", status, &reply)
			client.Close()
		}
	}
}

func flattenCSV(inPath, outPath string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()

	rows, err := csv.NewReader(in).ReadAll()
	if err != nil {
		return err
	}

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	w := csv.NewWriter(out)
	defer w.Flush()

	if err := w.Write(rows[0]); err != nil {
		return err
	}

	var set, live string
	for _, rec := range rows[1:] {
		// normalize to at least 3 columns
		for len(rec) < 3 {
			rec = append(rec, "")
		}
		if rec[0] != "" {
			set = rec[0]
		}
		if rec[2] != "" {
			live = rec[2]
		}
		if err := w.Write([]string{set, rec[1], live}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	//Input Processing
	if err := flattenCSV("Input/CSE535-F25-Project-1-Testcases-1.csv",
		"Input/CSE535-F25-Project-1-Testcases_flat.csv"); err != nil {
		log.Fatal(err)
	}

	f, err := os.Open("Input/CSE535-F25-Project-1-Testcases_flat.csv")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}

	setNumber_to_orderedItems := make(map[string][]OrderedItem)
	setNumber_to_livenodes := make(map[string][]int)

	//Prasing Input CSV file
	//1. To get live nodes for each set
	//2. Transactions to perform for each set
	//3. Order transactions based on the possion of "LF" command
	for _, record := range records[1:] {

		//Set Number
		setNumber := record[0]

		//Transaction
		transaction := record[1]

		// Handle LF commands
		if transaction == "LF" {
			setNumber_to_orderedItems[setNumber] = append(setNumber_to_orderedItems[setNumber], OrderedItem{IsLF: true})
			continue
		}

		if !strings.Contains(transaction, "(") || !strings.Contains(transaction, ")") {
			continue
		}

		parts := strings.Split(strings.Trim(transaction, "()"), ",")
		if len(parts) < 3 {
			continue
		}

		Sender := strings.TrimSpace(parts[0])
		Receiver := strings.TrimSpace(parts[1])
		Amount := strings.TrimSpace(parts[2])
		amountInt, _ := strconv.Atoi(Amount)

		setNumber_to_orderedItems[setNumber] = append(setNumber_to_orderedItems[setNumber], OrderedItem{
			IsLF: false,
			Transaction: configurations.Transaction{
				Sender:   Sender,
				Receiver: Receiver,
				Amount:   amountInt,
			},
		})

		//Live Nodes
		List_of_Livenodes := []int{}
		liveNodes := record[2]
		nodes := strings.Split(strings.Trim(liveNodes, "[]"), ",")
		for _, node := range nodes {
			nodeName := strings.TrimSpace(node)
			if len(nodeName) > 1 && nodeName[0] == 'n' {
				nodeInt, _ := strconv.Atoi(nodeName[1:])
				List_of_Livenodes = append(List_of_Livenodes, nodeInt)
			}
		}
		setNumber_to_livenodes[setNumber] = List_of_Livenodes
	}

	setNumber_to_txns := make(map[string][]configurations.Transaction)
	for setnum, items := range setNumber_to_orderedItems {
		for _, item := range items {
			if !item.IsLF {
				setNumber_to_txns[setnum] = append(setNumber_to_txns[setnum], item.Transaction)
			}
		}
	}

	last_leader := 1 //Node 1 is the leader while system initialization
	timestamp := make(map[string]int)
	var timestampMutex sync.RWMutex
	for i := 'A'; i <= 'J'; i++ {
		timestamp[string(i)] = 1
	}

	// Sort set numbers numerically
	setNumbers := make([]string, 0, len(setNumber_to_orderedItems))
	for setnum := range setNumber_to_orderedItems {
		setNumbers = append(setNumbers, setnum)
	}
	for i := 0; i < len(setNumbers); i++ {
		for j := i + 1; j < len(setNumbers); j++ {
			num1, _ := strconv.Atoi(setNumbers[i])
			num2, _ := strconv.Atoi(setNumbers[j])
			if num1 > num2 {
				setNumbers[i], setNumbers[j] = setNumbers[j], setNumbers[i]
			}
		}
	}

	//Start processing transactions set by set
	for _, setnum := range setNumbers {

		orderedItems := setNumber_to_orderedItems[setnum]
		// Only one node can be leader at a time, so track a single failed leader
		failedLeader := 0

		RPCCall_FailNodes(setNumber_to_livenodes[setnum])

		if setnum != "1" {
			// Wait briefly for election to happen after failing/restoring leaders
			time.Sleep(5 * time.Second)
		}

		// Split into batches separated by LF
		currentBatch := []configurations.Transaction{}
		for _, item := range orderedItems {
			if item.IsLF {
				// Process current batch if not empty
				if len(currentBatch) > 0 {
					processTransactionBatch(currentBatch, &last_leader, timestamp, &timestampMutex)
					currentBatch = []configurations.Transaction{}
				}
				// Before failing the current leader for this LF, restore any earlier failed leader
				// so it is back online for the subsequent transactions between LFs.
				if failedLeader != 0 {
					restoreFailedLeader(failedLeader)
					// clear after restoring
					failedLeader = 0
				}
				// Trigger leader failure
				triggerLeaderFailure(last_leader)
				// record the failed leader
				failedLeader = last_leader
			} else {
				// Add transaction to current batch
				currentBatch = append(currentBatch, item.Transaction)
			}
		}

		// Process final batch if not empty
		if len(currentBatch) > 0 {
			processTransactionBatch(currentBatch, &last_leader, timestamp, &timestampMutex)
		}

		// Restore only failed leader at set end (if any)
		if failedLeader != 0 {
			restoreFailedLeader(failedLeader)
			failedLeader = 0
		}

		// Small pause between sets to allow system to settle
		time.Sleep(1 * time.Second)

		fmt.Printf("Do you want to continue to the next set? (y/n): ")
		var input string
		fmt.Scanln(&input)
		if input != "y" {
			break
		}

	}

}
