# Multi-Paxos Protocol for Banking Application

## Introduction

This project implements a distributed banking system using the Multi-Paxos consensus protocol. The system consists of multiple nodes that coordinate to maintain consistency across distributed transactions while handling node failures and leader elections. The implementation demonstrates fault-tolerant distributed computing principles through a practical banking application where clients can transfer funds between accounts.

## Language and Modules Used

The project is implemented in **Go (Golang)** and utilizes the following core modules:

- **net/rpc**: Remote Procedure Call implementation for inter-node communication
- **net/http**: HTTP server functionality for RPC endpoints
- **database/sql**: Database interface for persistent storage
- **encoding/csv**: CSV file processing for test case input
- **sync**: Synchronization primitives (mutexes, wait groups) for concurrent operations
- **time**: Timer management for election timeouts and proposal timeouts
- **modernc.org/sqlite**: SQLite database driver for persistent storage

## Project Structure

```
Project1/
├── Configurations/
│   └── configurations.go
├── Input/
│   ├── CSE535-F25-Project-1-Testcases-1.csv
│   └── CSE535-F25-Project-1-Testcases_flat.csv
├── System/
│   ├── Database/
│   ├── Logs/
│   ├── logger.go
│   ├── node.go
│   ├── server_calls.go
│   └── node.exe
└── main.go
```

## Component Summary

### Configurations
- **configurations.go**: Contains system-wide configuration settings including node ports, timeouts, client configurations, and database initialization. Defines data structures for transactions, ballot numbers, and consensus protocol messages.

### Input
- **Test case files**: CSV files containing transaction sequences and node failure scenarios for testing the distributed system's behavior under various conditions.

### System
- **node.go**: Implements the core node lifecycle management including initialization, leader election, and new-view protocols. Handles the Multi-Paxos consensus algorithm phases.

- **server_calls.go**: Contains all RPC method implementations including ClientRequest, Accept, Prepare, NewView, Commit, and FailNode. Manages the consensus protocol communication between nodes.

- **logger.go**: Provides logging functionality for debugging and monitoring system behavior during consensus operations.

- **Database/**: Directory for SQLite database files storing account balances and transaction history for each node.

- **Logs/**: Directory containing log files for system monitoring and debugging.

### Main Application
- **main.go**: Client-side implementation that processes test cases, manages transaction batches, handles leader failures, and coordinates with the distributed nodes through RPC calls.

## How to Run the Project

### Prerequisites
- Go 1.21 or higher
- SQLite support

### Setup and Execution

1. **Start the distributed nodes** (run each in a separate terminal):
   ```bash
   cd Project1/System
   go run . -id=1
   go run . -id=2
   go run . -id=3
   go run . -id=4
   go run . -id=5
   ```

2. **Run the client application**:
   ```bash
   cd Project1
   go run main.go
   ```

3. **Monitor system behavior**:
   - Each node provides an interactive menu to view logs, database state, and transaction status
   - The client will process test cases and prompt for continuation between sets
   - System automatically handles leader elections and node failures as specified in test cases

### Test Case Format
The system processes CSV files containing:
- Transaction sequences: `(Sender, Receiver, Amount)`
- Leader failure commands: `LF`
- Node availability configurations: `[n1, n2, n3, n4, n5]`

## Acknowledgement

AI code helper was used while development of the project in debugging, error resolving, building helper modules and getting insights on some of the system design part.
I would like to thank TA Kevin Dharmawan for resolving some doubts related to client implimentation.