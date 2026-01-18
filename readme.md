# Distributed Banking Paxos (Multi-Paxos)

<!-- Tech Stack Badges -->
![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-07405E?style=for-the-badge&logo=sqlite&logoColor=white)
![Paxos](https://img.shields.io/badge/Consensus-Multi_Paxos-FF6F00?style=for-the-badge)
![Distributed Systems](https://img.shields.io/badge/System-Distributed-critical?style=for-the-badge)

## 📌 Introduction

This project implements a robust **Distributed Banking System** based on the **Multi-Paxos** consensus protocol. It demonstrates core principles of distributed computing including:
- **Strong Consistency**: Ensuring all nodes agree on the order of transactions.
- **Fault Tolerance**: Continuing operations despite node failures.
- **Leader Election**: Dynamic handling of leader failures.

The system simulates a banking environment where clients can perform atomic fund transfers between accounts across a distributed network of nodes.

## 🚀 Key Features

*   **Multi-Paxos Consensus**: rigorously implements the Phases (Prepare, Accept, Commit) for agreement.
*   **Dynamic Leader Election**: Automatically detects leader failures and elects a new leader.
*   **Persistent Storage**: Uses SQLite to persist account states and transaction logs.
*   **Concurrency Control**: Efficiently handles concurrent client requests using Go routines and synchronization.
*   **Resiliency**: Withstands varying failure scenarios (e.g., partitioned network, crashed nodes).

## 🛠️ Technology Stack

- **Language**: Go (Golang)
- **Communication**: `net/rpc` over HTTP
- **Storage**: `modernc.org/sqlite`
- **Testing**: automated CSV-based test cases

## 📂 Project Structure

```bash
CFT/
├── Project1/
│   ├── Configurations/ # System protocols and constants
│   ├── System/         # Node implementation (Paxos logic)
│   ├── Input/          # Test case CSVs
│   ├── logger.go       # Structed logging
│   ├── node.go         # Core Node logic
│   └── main.go         # Entry point (Client)
├── go.mod
└── README.md
```

## ⚡ How to Run

### Prerequisites
*   Go 1.21 or higher installed.
*   GCC (for SQLite CGO).

### Steps

1.  **Start the Nodes**
    Open **5 separate terminals**, one for each node in the cluster. Run the following command in each, changing the `-id` from 1 to 5.

    ```bash
    cd Project1/System
    go run . -id=1
    ```
    *(Repeat for id=2, id=3, id=4, id=5)*

2.  **Start the Client**
    In a new terminal, run the client to submit transactions.

    ```bash
    cd Project1
    go run main.go
    ```

3.  **Interact**
    *   The client will process transactions from `Input/` CSV files.
    *   Monitor the node terminals to see the consensus process (Prepare, Accept, Commit logs).

## 🧪 Testing

The system is tested using scenarios defined in `Project1/Input/*.csv`. These include:
*   Normal transaction flow.
*   Leader failure triggers (`LF` command).
*   Node crashes and restarts.

## 🤝 Acknowledgements

*   Developed as part of **CSE 535: Distributed Systems**.
*   Special thanks to TA Kevin Dharmawan for guidance on client implementation.