package goraft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type Entry struct {
	Command []byte
	Term    uint64
}

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage

	// Candidate requesting vote
	CandidateId string

	// Index of candidate's last log entry
	LastLogIndex uint64

	// Term of candidate's last log entry
	LastLogTerm uint64
}

type RequestVoteResponse struct {
	RPCMessage

	// True means candidate received vote
	VoteGranted bool
}

type AppendEntriesRequest struct {
	RPCMessage

	// So follower can redirect clients
	LeaderId string

	// Index of log entry immediately preceding new ones
	PrevLogIndex uint64

	// Term of prevLogIndex entry
	PrevLogTerm uint64

	// Log entries to store. Empty for heartbeat.
	Entries []Entry

	// Leader's commitIndex
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	RPCMessage

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

type ClusterMember struct {
	Id      string
	Address string

	// Index of the next log entry to send
	nextIndex uint64
	// Highest log entry known to be replicated
	matchIndex uint64

	// TCP connection
	rpcClient *rpc.Client
}

func (c *ClusterMember) rpcCall(name string, req, rsp any) error {
	var err error
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		if err != nil {
			return err
		}
	}

	return c.rpcClient.Call(name, req, rsp)
}

type PersistentState struct {
	// The current term
	CurrentTerm uint64

	// candidateId that received vote in current term (or null if none)
	VotedFor string

	Log []Entry
}

type ServerState string

const (
	leaderState    ServerState = "leader"
	followerState              = "follower"
	candidateState             = "candidate"
)

type Server struct {
	mu sync.Mutex
	// ----------- PERSISTENT STATE -----------

	// The current term
	currentTerm uint64

	// candidateId that received vote in current term (or null if none)
	votedFor string

	log []Entry

	// ----------- READONLY STATE -----------

	// Unique identifier for this Server
	id string

	// The TCP address for RPC
	address string

	// When to start elections after no append entry messages
	heartbeatTimeout time.Duration

	// User-provided state machine
	statemachine StateMachine

	// Metadata directory
	metadataDir string

	// Metadata store
	fd *os.File

	// ----------- VOLATILE STATE -----------

	// Index of highest log entry known to be committed
	commitIndex uint64

	// Index of highest log entry applied to state machine
	lastApplied uint64

	// Candidate, follower, or leader
	state ServerState

	// Servers in the cluster, including this one
	cluster []ClusterMember

	// Index of this server
	clusterIndex int
}

func NewServer(
	cluster []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	s := &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		cluster:      cluster,
		statemachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
	}
	s.state = followerState
	return s
}

func (s *Server) persist() {
	s.fd.Truncate(0)
	s.fd.Seek(0, 0)
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(PersistentState{
		CurrentTerm: s.currentTerm,
		Log:         s.log,
		VotedFor:    s.votedFor,
	})
	s.debug(fmt.Sprintf("Term: %d. Log Len: %d. Voted For: %s.", s.currentTerm, len(s.log), s.votedFor))
	if err != nil {
		panic(err)
	}
	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
}

func (s *Server) restore() {
	s.fd.Seek(0, 0)
	dec := gob.NewDecoder(s.fd)
	var p PersistentState
	err := dec.Decode(&p)
	if err != nil {
		// Always must be one log entry
		if err == io.EOF {
			s.log = []Entry{{}}
		} else {
			panic(err)
		}
	} else {
		s.log = p.Log
		s.votedFor = p.VotedFor
		s.currentTerm = p.CurrentTerm
	}

	for i := range s.cluster {
		s.cluster[i].nextIndex = uint64(len(s.log))
	}
}

func (s *Server) RequestVote(req *RequestVoteRequest, rsp *RequestVoteResponse) error {
	panic("Unsupported")
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}

	return b
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %s] %s", time.Now().Format(time.RFC3339), s.id, msg)
}

func (s *Server) debug(msg string) {
	fmt.Println(s.debugmsg(msg))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}

func (s *Server) AppendEntries(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()

	rsp.Term = s.currentTerm
	rsp.Success = false

	logLen := uint64(len(s.log))

	validPreviousLog := req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm

	if req.Term < s.currentTerm || !validPreviousLog {
		s.mu.Unlock()
		return nil
	}

	if len(req.Entries) == 0 {
		s.mu.Unlock()
		rsp.Success = true
		return nil
	}

	validExistingEntry := req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm
	newEntry := req.PrevLogIndex >= logLen
	if !validExistingEntry && !newEntry {
		// Delete entry and all that follow it
		s.log = s.log[:req.PrevLogIndex]
	}

	Assert("Must be at least one log entry at all times", len(s.log) > 0, true)
	s.log = append(s.log[:req.PrevLogIndex+1], req.Entries...)
	s.persist()
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, logLen-1)
	}

	s.mu.Unlock()
	rsp.Success = true
	return nil
}

var ErrApplyToLeader = errors.New("Cannot apply message to follower, apply to leader.")

func (s *Server) Apply(command []byte) ([]byte, error) {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}

	s.log = append(s.log, Entry{
		Term:    s.currentTerm,
		Command: command,
	})
	s.persist()
	lastLogIndex := uint64(len(s.log) - 1)
	s.mu.Unlock()

	// Leader already has it in the log, so just need exactly floor(cluster.len / 2)m
	commitsNeeded := (len(s.cluster) / 2) + 1
	var commitsNeededMu sync.Mutex
	committed := make(chan bool)
	if len(s.cluster) == 0 {
		go func() {
			committed <- true
		}()
	}

	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(server *ClusterMember) {
			for {
				s.mu.Lock()
				if lastLogIndex < server.nextIndex {
					s.mu.Unlock()
					return
				}

				req := AppendEntriesRequest{
					LeaderId:     s.cluster[s.clusterIndex].Id,
					PrevLogIndex: server.nextIndex - 1,
					PrevLogTerm:  s.log[server.nextIndex-1].Term,
					Entries:      s.log[server.nextIndex : lastLogIndex+1],
					LeaderCommit: s.commitIndex,
				}
				Server_assert(s, "Sending at least one entry", len(req.Entries) > 0, true)

				s.mu.Unlock()

				var rsp AppendEntriesResponse
				err := server.rpcCall("Server.AppendEntries", req, &rsp)
				if err != nil {
					panic("Unexpected RPC failure: " + err.Error())
				}

				s.mu.Lock()
				if rsp.Success {
					prev := server.nextIndex
					server.nextIndex = lastLogIndex + 1
					server.matchIndex = lastLogIndex + 2
					s.debug(fmt.Sprintf("Message accepted for %s. Prev Index: %d Next Index: %d.", server.Id, prev, server.nextIndex))

				} else {
					s.debug(fmt.Sprintf("Forced to go back to %d for: %s\n", server.nextIndex-1, server.Id))
					server.nextIndex--
					req.PrevLogIndex--
				}
				s.mu.Unlock()

				if rsp.Success {
					commitsNeededMu.Lock()

					if commitsNeeded > 0 {
						commitsNeeded--
						if commitsNeeded == 0 {
							committed <- true
						}
					}

					commitsNeededMu.Unlock()
					break
				}
			}
		}(&s.cluster[i])
	}

	<-committed

	s.mu.Lock()
	s.commitIndex = lastLogIndex
	s.lastApplied = s.commitIndex
	res, err := s.statemachine.Apply(command)
	s.mu.Unlock()

	return res, err
}

// Make sure rand is seeded
func (s *Server) Start() {
	var err error
	s.fd, err = os.OpenFile(path.Join(s.metadataDir, "md_"+s.id+".dat"), os.O_SYNC|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	s.restore()
	Server_assert(s, "At least one log entry", len(s.log) > 0, true)

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	go http.Serve(l, mux)

	//for {
	//	s.mu.Lock()
	//	state := s.state
	//}
}

// TODO: delete this
func (s *Server) MakeLeader() {
	s.mu.Lock()
	s.state = leaderState
	s.mu.Unlock()
}

func (s *Server) Entries() int {
	s.mu.Lock()
	e := len(s.log)
	s.mu.Unlock()
	return e
}
