package goraft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	//"sync"
	sync "github.com/sasha-s/go-deadlock"
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

type applyResult struct {
	res []byte
	err error
}

type Entry struct {
	Command []byte
	Term    uint64
	result  chan applyResult
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

	votedFor string

	// TCP connection
	rpcClient *rpc.Client
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
	Debug bool

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
	electionTimeout time.Time

	// How often to send empty messages
	heartbeatMs int

	// When to next send empty message
	heartbeatTimeout time.Time

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

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}

	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}

	return b
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %s, Term: %d] %s", time.Now().Format(time.RFC3339), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
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
		heartbeatMs:  100,
	}
	s.state = followerState
	return s
}

// Weird thing to note is that writing to a deleted disk is not an
// error on Linux. So if these files are deleted, you won't know about
// that until the process restarts.
func (s *Server) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fd.Truncate(0)
	s.fd.Seek(0, 0)
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(PersistentState{
		CurrentTerm: s.currentTerm,
		Log:         s.log,
		VotedFor:    s.votedFor,
	})
	if err != nil {
		panic(err)
	}
	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debug(fmt.Sprintf("Persisted. Term: %d. Log Len: %d. Voted For: %s.", s.currentTerm, len(s.log), s.votedFor))
}

func (s *Server) restore() {
	s.fd.Seek(0, 0)
	dec := gob.NewDecoder(s.fd)
	var p PersistentState
	err := dec.Decode(&p)
	if err != nil {
		// Always must be one log entry
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			s.log = []Entry{{}}
		} else {
			panic(err)
		}
	} else {
		s.log = p.Log
		s.votedFor = p.VotedFor
		s.currentTerm = p.CurrentTerm
	}
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		go func(server *ClusterMember) {
			s.mu.Lock()

			// Skip if vote already requested
			if server.votedFor != "" {
				s.mu.Unlock()
				return
			}

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: uint64(len(s.log) - 1),
				LastLogTerm:  s.log[len(s.log)-1].Term,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(server, "Server.HandleRequestVoteRequest", req, &rsp)
			if !ok {
				// Will retry later
				return
			}

			s.mu.Lock()
			dropStaleResponse := rsp.Term < s.currentTerm
			if rsp.VoteGranted && !dropStaleResponse {
				s.debug(fmt.Sprintf("Vote granted by %s", server.Id))
				server.votedFor = s.id
			}
			s.mu.Unlock()
		}(&s.cluster[i])
	}
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.updateTerm(req.RPCMessage)

	s.mu.Lock()

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	var grant bool
	if req.Term < s.currentTerm {
		Server_assert(s, "VoteGranted = false", rsp.VoteGranted, false)
	} else {
		logOk := req.LastLogTerm > s.log[len(s.log)-1].Term ||
			(req.LastLogTerm == s.log[len(s.log)-1].Term && req.LastLogIndex >= uint64(len(s.log)-1))
		grant = req.Term == s.currentTerm &&
			logOk &&
			(s.votedFor == "" || s.votedFor == req.CandidateId)
		if grant {
			s.debug(fmt.Sprintf("Voted for %s for term %d", req.CandidateId, s.currentTerm))
			s.votedFor = req.CandidateId
			rsp.VoteGranted = true
		}
	}

	s.mu.Unlock()

	if grant {
		s.persist()
	}

	return nil
}

func (s *Server) updateTerm(msg RPCMessage) {
	s.mu.Lock()

	persist := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.votedFor = ""
		persist = true
		s.debug("Transitioned to follower")
	}
	s.mu.Unlock()

	if persist {
		s.persist()
	}
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.updateTerm(req.RPCMessage)

	s.mu.Lock()

	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	logLen := uint64(len(s.log))

	validPreviousLog := req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm

	if req.Term < s.currentTerm || !validPreviousLog {
		return nil
	}

	s.mu.Unlock()

	s.resetElectionTimeout()

	s.mu.Lock()

	if len(req.Entries) == 0 {
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
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, logLen-1)

		// Also set own matchIndex
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].matchIndex = s.commitIndex
			}
		}
	}

	s.mu.Unlock()
	s.persist()

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

	resultChan := make(chan applyResult)
	s.log = append(s.log, Entry{
		Term:    s.currentTerm,
		Command: command,
		result:  resultChan,
	})
	s.mu.Unlock()
	s.persist()

	// TODO: What happens if this takes too long?
	result := <-resultChan
	return result.res, result.err
}

func (s *Server) rpcCall(c *ClusterMember, name string, req, rsp any) bool {
	s.mu.Lock()
	var err error
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
	}
	s.mu.Unlock()

	if err == nil {
		err = c.rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warn(fmt.Sprintf("Error calling %s on %s: %s", name, c.Id, err))
	}

	return err == nil
}

func (s *Server) appendEntries() {
	s.mu.Lock()
	lastLogIndex := uint64(len(s.log) - 1)
	s.mu.Unlock()

	for i := range s.cluster {
		// Don't need to send message to self
		if i == s.clusterIndex {
			continue
		}

		go func(server *ClusterMember) {
			for {
				s.mu.Lock()
				timeForHeartbeat := time.Now().After(s.heartbeatTimeout)
				if lastLogIndex < server.matchIndex || timeForHeartbeat {
					s.mu.Unlock()
					return
				}

				s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)

				req := AppendEntriesRequest{
					LeaderId:     s.cluster[s.clusterIndex].Id,
					PrevLogIndex: server.nextIndex - 1,
					PrevLogTerm:  s.log[server.nextIndex-1].Term,
					Entries:      s.log[server.nextIndex : lastLogIndex+1],
					LeaderCommit: s.commitIndex,
				}

				s.mu.Unlock()

				var rsp AppendEntriesResponse
				ok := s.rpcCall(server, "Server.HandleAppendEntriesRequest", req, &rsp)
				if !ok {
					// Will retry later
					return
				}

				s.mu.Lock()
				dropStaleResponse := rsp.Term < s.currentTerm
				if rsp.Success && !dropStaleResponse {
					prev := server.nextIndex
					server.nextIndex = lastLogIndex + 1
					server.matchIndex = lastLogIndex + 1
					s.debug(fmt.Sprintf("Message accepted for %s. Prev Index: %d Next Index: %d.", server.Id, prev, server.nextIndex))
					s.mu.Unlock()
					return
				} else {
					server.nextIndex = max(server.nextIndex-1, 1)
					req.PrevLogIndex = max(req.PrevLogIndex-1, 1)
					s.debug(fmt.Sprintf("Forced to go back to %d for: %s\n", server.nextIndex, server.Id))
				}
				s.mu.Unlock()
			}
		}(&s.cluster[i])
	}
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := uint64(len(s.log) - 1); i > s.commitIndex; i-- {
		quorum := len(s.cluster)/2 + 1
		for _, server := range s.cluster {
			if quorum == 0 {
				break
			}

			if server.matchIndex >= i {
				quorum--
			}
		}

		if quorum == 0 {
			s.commitIndex = i
			s.debug(fmt.Sprintf("New commit index: %d", i))
			break
		}
	}

	for s.lastApplied <= s.commitIndex {
		if s.lastApplied == 0 {
			// First entry is always a blank one.
			s.lastApplied++
			continue
		}

		s.debug(fmt.Sprintf("Applying %d\n", s.lastApplied))
		log := s.log[s.lastApplied]
		// TODO: what if Apply() takes too long?
		res, err := s.statemachine.Apply(log.Command)
		if log.result != nil {
			log.result <- applyResult{
				res: res,
				err: err,
			}
		}

		s.lastApplied++
	}
}

func (s *Server) resetElectionTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Random timeout between 150-300ms (by default)
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()

	hasTimedOut := time.Now().After(s.electionTimeout)
	if hasTimedOut {
		s.state = candidateState
		s.currentTerm++
		s.votedFor = s.id
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
			} else {
				s.cluster[i].votedFor = ""
			}
		}
	}
	s.mu.Unlock()

	if hasTimedOut {
		s.resetElectionTimeout()
		s.persist()
	}

	s.requestVote()
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if quorum == 0 {
			break
		}

		if s.cluster[i].votedFor == s.id {
			quorum--

			// Reset all other state while we're at it
			s.cluster[i].nextIndex = uint64(len(s.log) + 1)
			s.cluster[i].matchIndex = 0
		}
	}

	if quorum == 0 {
		s.debug(fmt.Sprintf("New leader for term %d", s.currentTerm))
		s.state = leaderState
	}
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

	go func() {
		ticker := time.NewTicker(time.Millisecond)

		for range ticker.C {
			s.mu.Lock()
			state := s.state
			s.mu.Unlock()

			switch state {
			case leaderState:
				s.appendEntries()
				s.advanceCommitIndex()
			case followerState:
				s.timeout()
			case candidateState:
				s.timeout()
				s.becomeLeader()
			}
		}
	}()
}

func (s *Server) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == leaderState
}

func (s *Server) Entries() int {
	s.mu.Lock()
	e := len(s.log)
	s.mu.Unlock()
	return e
}
