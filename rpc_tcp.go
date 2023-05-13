package goraft

import (
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type TCPRPCProxy struct {
	s           *Server
	mu          sync.Mutex
	connections map[int]*rpc.Client
}

func NewTCPRPCProxy(s *Server) *TCPRPCProxy {
	return &TCPRPCProxy{
		s:           s,
		mu:          sync.Mutex{},
		connections: map[int]*rpc.Client{},
	}
}

func (trp *TCPRPCProxy) Start() {
	l, err := net.Listen("tcp", trp.s.address)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			// Listen for an incoming connection.
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting: ", err.Error())
				continue
			}

			go trp.handleRequest(conn)
		}
	}()
}

type requestKind uint

const (
	appendEntriesRequest requestKind = iota
	requestVoteRequest
)

func (trp *TCPRPCProxy) handleRequest(conn *net.Conn) {
	var buf [1024]byte
	err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading: ", err.Error())
		return
	}

	switch buf[0] {
	case appendEntriesRequest:
		trp.handleAppendEntries(conn, buf)
	case requestVoteRequest:
		trp.handleRequestVote(conn, buf)
	default:
		fmt.Println("Unknown request type.")
	}
}

func (trp *TCPRPCProxy) handleAppendEntries(conn *net.Conn, buf [1024]byte) {
	// AppendEntriesRequest
	// Bytes 0  - 1:  Request type
	// Bytes 1  - 9:  Term
	// Bytes 9  - 17: LeaderId
	// Bytes 17 - 25: PrevLogIndex
	// Bytes 25 - 33: PrevLogTerm
	// Bytes 33 - 41: LeaderCommit
	// Bytes 41 - 49: Number of entries
	// Bytes 49 - N:  Entries

	var req AppendEntriesRequest
	req.Term = binary.LittleEndian.Uint64(buf[1:9])
	req.LeaderId = binary.LittleEndian.Uint64(buf[9:17])
	req.PrevLogIndex = binary.LittleEndian.Uint64(buf[17:25])
	req.PrevLogTerm = binary.LittleEndian.Uint64(buf[25:33])
	req.LeaderCommit = binary.LittleEndian.Uint64(buf[33:41])
	nEntries := binary.LittleEndian.Uint64(buf[41:49])

	var e Entry
	for nEntries > 0 {
		nEntries--

		// Entry
		// Bytes 0  - 8:  Term
		// Bytes 8  - 16: Command length
		// Bytes 16 - N:  Command
		req.Term = binary.LittleEndian.Uint64(buf[1:9])
		req.LeaderId = binary.LittleEndian.Uint64(buf[9:17])

		// TODO: Finish this up

	}

	var rsp AppendEntriesResponse
	trp.s.HandleAppendEntriesRequest(req, &rsp)

	// AppendEntriesResponse
	// Bytes 0  - 1:  Request type
	// Bytes 1  - 9:  Term
	// Bytes 9     : VoteGranted
	binary.LittleEndian.PutUint64(buf[1:9], rsp.Term)
	buf[9] = rsp.VoteGranted

	err := conn.Write(buf)
	if err != nil {
		fmt.Println("Error writing: ", err.Error())
	}
}

func (trp *TCPRPCProxy) handleRequestVote(conn *net.Conn, buf [1024]byte) {
	// RequestVoteRequest
	// Bytes 0  - 1:  Request type
	// Bytes 1  - 9:  Term
	// Bytes 9  - 17: CandidateId
	// Bytes 17 - 25: LastLogIndex
	// Bytes 25 - 33: LastLogTerm

	var req RequestVoteRequest
	req.Term = binary.LittleEndian.Uint64(buf[1:9])
	req.CandidateId = binary.LittleEndian.Uint64(buf[9:17])
	req.LastLogIndex = binary.LittleEndian.Uint64(buf[17:25])
	req.LastLogTerm = binary.LittleEndian.Uint64(buf[25:33])

	var rsp RequestVoteResponse
	trp.s.HandleRequestVoteRequest(req, &rsp)

	// RequestVoteResponse
	// Bytes 0  - 1:  Request type
	// Bytes 1  - 9:  Term
	// Bytes 9     : VoteGranted
	binary.LittleEndian.PutUint64(buf[1:9], rsp.Term)
	buf[9] = rsp.VoteGranted

	err := conn.Write(buf)
	if err != nil {
		fmt.Println("Error writing: ", err.Error())
	}
}

func (trp *TCPRPCProxy) AppendEntries(clusterIndex int, req AppendEntriesRequest, rsp *AppendEntriesResponse) bool {
	// TODO:  implement this

}

func (trp *TCPRPCProxy) RequestVote(clusterIndex int, req RequestVoteRequest, rsp *RequestVoteResponse) bool {
	// TODO: implement this
}
