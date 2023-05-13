package goraft

import (
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type SlowRPCProxy struct {
	s           *Server
	mu          sync.Mutex
	connections map[int]*rpc.Client
}

func NewSlowRPCProxy(s *Server) *SlowRPCProxy {
	return &SlowRPCProxy{
		s:           s,
		mu:          sync.Mutex{},
		connections: map[int]*rpc.Client{},
	}
}

func (srp *SlowRPCProxy) Start() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(srp.s)
	l, err := net.Listen("tcp", srp.s.address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	go http.Serve(l, mux)
}

func (srp *SlowRPCProxy) rpcCall(clusterIndex int, name string, req, rsp any) bool {
	if rand.Intn(100) > 10 {
		time.Sleep(5 * time.Second)
	}

	srp.mu.Lock()
	conn, ok := srp.connections[clusterIndex]
	var err error
	if !ok {
		conn, err = rpc.DialHTTP("tcp", srp.s.cluster[clusterIndex].Address)
		// err is handled below
		srp.connections[clusterIndex] = conn
	}
	srp.mu.Unlock()

	if err == nil {
		err = conn.Call(name, req, rsp)
	} else {
		srp.s.warnf("Error calling %s on %d: %s.", name, srp.s.cluster[clusterIndex].Id, err)
	}

	return err == nil
}

func (srp *SlowRPCProxy) AppendEntries(clusterIndex int, req AppendEntriesRequest, rsp *AppendEntriesResponse) bool {
	return srp.rpcCall(clusterIndex, "Server.HandleAppendEntriesRequest", req, rsp)
}

func (srp *SlowRPCProxy) RequestVote(clusterIndex int, req RequestVoteRequest, rsp *RequestVoteResponse) bool {
	return srp.rpcCall(clusterIndex, "Server.HandleRequestVoteRequest", req, rsp)
}
