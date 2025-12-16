// System Configurations
// 1. For Client
// 2. For Servers/Nodes
// 3. For Persistant Database
package configurations

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

type Reply struct {
	B         BallotNumber
	Timestamp int
	Result    bool
	Msg       string
}

type TxnReply struct {
	Res bool
	Msg string
}

type Transaction struct {
	Sender    string
	Receiver  string
	Amount    int
	Reply     Reply
	Timestamp int
}

type BallotNumber struct {
	B      int
	NodeID int
}

type AcceptLog struct {
	AcceptNum BallotNumber
	AcceptSeq int
	AcceptVal Transaction
	Status    string
}

type NewViewInput struct {
	B         BallotNumber
	AcceptLog []AcceptLog
}

type Promise struct {
	AcceptedMsgs []AcceptLog
	Vote         int
}

type AcceptTxn struct {
	B          BallotNumber
	Txn        Transaction
	SeqNo      int
	Acceptance int
	Status     string
}

type NodeConfig struct {
	ID                int
	Port              int
	Bnum              BallotNumber
	AcceptLog         []AcceptLog
	ClientLastReply   map[string]int
	TxnsProcessed     map[Transaction]Reply
	T                 time.Duration
	Tp                time.Duration
	IsLive            bool
	SequenceNumber    int
	Timestamp         int
	LastExecuted      int
	PendingCommands   map[int]AcceptTxn
	DB                BankDatabase
	TransactionStatus map[int]string
}

type BankDatabase struct {
	db     *sql.DB
	nodeID int
}

func InitializeDB() BankDatabase {
	return BankDatabase{}
}

func InitializeDBWithNodeID(nodeID int) BankDatabase {

	os.MkdirAll("Database", 0755)

	dbFile := fmt.Sprintf("Database/node_%d.db", nodeID)
	os.Remove(dbFile)

	dbPath := fmt.Sprintf("Database/node_%d.db?_busy_timeout=5000&_journal_mode=WAL", nodeID)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database for node %d: %v", nodeID, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS accounts (
		client_id TEXT PRIMARY KEY,
		balance INTEGER NOT NULL
	);
	`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create accounts table for node %d: %v", nodeID, err)
	}

	clients := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for _, client := range clients {
		_, err = db.Exec("INSERT OR IGNORE INTO accounts (client_id, balance) VALUES (?, ?)", client, 10)
		if err != nil {
			log.Printf("Warning: Failed to initialize account %s for node %d: %v", client, nodeID, err)
		}
	}

	return BankDatabase{db: db, nodeID: nodeID}
}

func (db *BankDatabase) UpdateEntry(args Transaction, reply *bool) error {
	if db.db == nil {
		*reply = false
		return errors.New("database not initialized")
	}

	tx, err := db.db.Begin()
	if err != nil {
		*reply = false
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	var senderBalance int
	err = tx.QueryRow("SELECT balance FROM accounts WHERE client_id = ?", args.Sender).Scan(&senderBalance)
	if err != nil {
		*reply = false
		return fmt.Errorf("failed to get sender balance: %v", err)
	}

	if senderBalance < args.Amount {
		*reply = false
		return errors.New("insufficient funds")
	}

	_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE client_id = ?", args.Amount, args.Sender)
	if err != nil {
		*reply = false
		return fmt.Errorf("failed to update sender balance: %v", err)
	}

	_, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE client_id = ?", args.Amount, args.Receiver)
	if err != nil {
		*reply = false
		return fmt.Errorf("failed to update receiver balance: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		*reply = false
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	tx = nil

	*reply = true
	return nil
}

func (db *BankDatabase) PrintDB() {
	if db.db == nil {
		fmt.Println("Database not initialized")
		return
	}

	fmt.Println("+----------+----------+")
	fmt.Println("| SenderID | Balance  |")
	fmt.Println("+----------+----------+")

	rows, err := db.db.Query("SELECT client_id, balance FROM accounts ORDER BY client_id")
	if err != nil {
		fmt.Printf("Error querying database: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var clientID string
		var balance int
		err := rows.Scan(&clientID, &balance)
		if err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			continue
		}
		fmt.Printf("| %-8s | %-8d |\n", clientID, balance)
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("Error iterating rows: %v\n", err)
	}

	fmt.Println("+----------+----------+")
	fmt.Printf("\n")
}

func (db *BankDatabase) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

type ClientConfig struct {
	Name    string
	ID      int
	Timeout time.Duration
}

var Nodes = map[int]NodeConfig{
	1: {ID: 1, Port: 4040},
	2: {ID: 2, Port: 4041},
	3: {ID: 3, Port: 4042},
	4: {ID: 4, Port: 4043},
	5: {ID: 5, Port: 4044},
}

// Initialize Timers
// 1. Election Timeouts
// 2. Tp Timers
func init() {
	for id, node := range Nodes {
		// Election timeouts (milliseconds): reduced from 10-15s to 1.5-3s
		// Keep Tp = T/4 so proposal timeout remains shorter than election timeout
		minTimeout := 1500
		maxTimeout := 3000
		ms := rand.Intn(maxTimeout-minTimeout) + minTimeout
		node.T = time.Duration(ms) * time.Millisecond
		node.Tp = node.T / 4
		node.TransactionStatus = make(map[int]string)
		Nodes[id] = node
	}
}

var Clients = map[string]ClientConfig{
	// Reduced client timeouts to 8s to match faster election/proposal timing
	"A": {ID: 1, Timeout: 8 * time.Second},
	"B": {ID: 2, Timeout: 8 * time.Second},
	"C": {ID: 3, Timeout: 8 * time.Second},
	"D": {ID: 4, Timeout: 8 * time.Second},
	"E": {ID: 5, Timeout: 8 * time.Second},
	"F": {ID: 6, Timeout: 8 * time.Second},
	"G": {ID: 7, Timeout: 8 * time.Second},
	"H": {ID: 8, Timeout: 8 * time.Second},
	"I": {ID: 9, Timeout: 8 * time.Second},
	"J": {ID: 10, Timeout: 8 * time.Second},
}

func GetNodePort(id int) int {
	if cfg, ok := Nodes[id]; ok {
		return cfg.Port
	}
	return 0
}

func GetNodeElectionTimeout(id int) time.Duration {
	if cfg, ok := Nodes[id]; ok {
		return cfg.T
	}
	return 0
}

func GetNodeProposalTimeout(id int) time.Duration {
	if cfg, ok := Nodes[id]; ok {
		return cfg.Tp
	}
	return 0
}

func GetClientID(name string) int {
	if cfg, ok := Clients[name]; ok {
		return cfg.ID
	}
	return 0
}

func GetClientTimeout(name string) time.Duration {
	if cfg, ok := Clients[name]; ok {
		return cfg.Timeout
	}
	return 0
}

func (n *NodeConfig) GetTransactionStatus(seqNo int) string {
	if status, ok := n.TransactionStatus[seqNo]; ok {
		return status
	}
	return "X"
}
