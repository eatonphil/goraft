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
	voted    bool

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

	// Who was voted for
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
	return fmt.Sprintf("%s [Id: %s, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
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
	clusterConfig []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	sync.Opts.DeadlockTimeout = 50*time.Millisecond
	// Explicitly make a copy of the cluster because we'll be
	// modifying it in this server.
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		cluster = append(cluster, c)
	}

	s := &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		cluster:      cluster,
		statemachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  300,
		mu:           sync.Mutex{},
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
			s.log = []Entry{}
		} else {
			panic(err)
		}
	} else {
		s.log = p.Log
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.votedFor = p.VotedFor
			}
		}
		s.currentTerm = p.CurrentTerm
	}
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			// Skip if vote already requested
			if s.cluster[i].voted {
				s.mu.Unlock()
				return
			}
			s.debug("Requesting vote from " + s.cluster[i].Id + ".")
			s.cluster[i].voted = true

			var lastLogIndex, lastLogTerm uint64
			if len(s.log) > 0 {
				lastLogIndex = uint64(len(s.log) - 1)
				lastLogTerm = s.log[len(s.log)-1].Term
			}

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp)
			if !ok {
				// Will retry later
				return
			}
			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			dropStaleResponse := rsp.Term != req.Term && s.state == leaderState
			if dropStaleResponse {
				return
			}

			if rsp.VoteGranted {
				s.debug(fmt.Sprintf("Vote granted by %s.", s.cluster[i].Id))
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.updateTerm(req.RPCMessage)

	s.mu.Lock()
	s.debug("Received vote request from " + req.CandidateId + ".")

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	var grant bool
	if req.Term < s.currentTerm {
		s.debug("Not granting vote request from " + req.CandidateId + ".")
		Server_assert(s, "VoteGranted = false", rsp.VoteGranted, false)
	} else {
		var lastLogTerm, logLen uint64
		if len(s.log) > 0 {
			lastLogTerm = s.log[len(s.log)-1].Term
			logLen = uint64(len(s.log) - 1)
		}
		logOk := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)
		grant = req.Term == s.currentTerm &&
			logOk &&
			(s.votedFor == "" || s.votedFor == req.CandidateId)
		if grant {
			s.debug(fmt.Sprintf("Voted for %s.", req.CandidateId))
			s.votedFor = req.CandidateId
			rsp.VoteGranted = true
		} else {
			s.debug(fmt.Sprintf("req.LastLogTerm: %d, lastLogTerm: %d", req.LastLogTerm, lastLogTerm))
			s.debug(fmt.Sprintf("lastLogIndex: %d, myLastLogIndex: %d", req.LastLogIndex, len(s.log)-1))
			s.debug(fmt.Sprintf("logOk: %t, votedFor: %s", logOk, s.votedFor))
			s.debug("Not granting vote request from " + req.CandidateId + ".")
		}
	}

	s.mu.Unlock()

	if grant {
		s.resetElectionTimeout()
		s.persist()
	}

	return nil
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	s.mu.Lock()

	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.votedFor = ""
		transitioned = true
		s.debug("Transitioned to follower")
	}
	s.mu.Unlock()

	if transitioned {
		s.resetElectionTimeout()
		s.persist()
	}
	return transitioned
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.updateTerm(req.RPCMessage)

	s.mu.Lock()

	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if req.Term < s.currentTerm {
		s.debug(fmt.Sprintf("Dropping request from old leader "+req.LeaderId+": term %d.", req.Term))
		s.mu.Unlock()
		// Not a valid leader.
		return nil
	}

	s.mu.Unlock()

	// Valid leader so reset election.
	s.resetElectionTimeout()

	s.mu.Lock()

	logLen := uint64(len(s.log))
	validPreviousLog := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex < logLen &&
			s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.debug("Not a valid log")
		s.mu.Unlock()
		return nil
	}

	validExistingEntry := req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm
	newEntry := req.PrevLogIndex >= logLen
	if !validExistingEntry && !newEntry {
		// Delete entry and all that follow it
		s.log = s.log[:req.PrevLogIndex]
		logLen = uint64(len(s.log))
	}

	s.log = append(s.log, req.Entries...)
	Assert("Log contains new entries", len(s.log), int(logLen) + len(req.Entries))
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
	s.debug("Processing new entry!")

	resultChan := make(chan applyResult)
	s.log = append(s.log, Entry{
		Term:    s.currentTerm,
		Command: command,
		result:  resultChan,
	})
	s.mu.Unlock()
	s.persist()

	s.appendEntries()

	// TODO: What happens if this takes too long?
	s.debug("Waiting to be applied!")
	result := <-resultChan
	return result.res, result.err
}

func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()

	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warn(fmt.Sprintf("Error calling %s on %s: %s", name, c.Id, err))
	}

	return err == nil
}

func (s *Server) appendEntries() {
	s.mu.Lock()
	lenLog := uint64(len(s.log))
	s.mu.Unlock()

	for i := range s.cluster {
		// Don't need to send message to self
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()
			var prevLogIndex, prevLogTerm uint64
			var logBegin uint64
			var entries []Entry
			if s.cluster[i].nextIndex > lenLog-1 {
				prevLogIndex = s.cluster[i].nextIndex-1
				prevLogTerm = s.log[prevLogIndex].Term
				logBegin = prevLogIndex + 1
			}

			entries = s.log[logBegin:]

			lenEntries := uint64(len(entries))
			req := AppendEntriesRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,	
				},
				LeaderId:     s.cluster[s.clusterIndex].Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			s.debug(fmt.Sprintf("Sending %d entries to %s for term %d.", len(entries), s.cluster[i].Id, req.Term))
			ok := s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp)
			if !ok {
				// Will retry later
				return
			}

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			dropStaleResponse := rsp.Term != req.Term && s.state == leaderState
			if dropStaleResponse {
				return
			}

			if rsp.Success {
				prev := s.cluster[i].nextIndex
				s.cluster[i].nextIndex = req.PrevLogIndex + lenEntries
				s.cluster[i].matchIndex = s.cluster[i].nextIndex-1
				s.debug(fmt.Sprintf("Message accepted for %s. Prev Index: %d Next Index: %d.", s.cluster[i].Id, prev, s.cluster[i].nextIndex))
			} else {
				s.cluster[i].nextIndex = min(s.cluster[i].nextIndex, 1)
				s.debug(fmt.Sprintf("Forced to go back to %d for: %s.", s.cluster[i].nextIndex, s.cluster[i].Id))
			}
		}(i)
	}
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()

	// Leader can update commitIndex on quorum.
	if s.state == leaderState {
		var lastLogIndex uint64
		if len(s.log) > 0 {
			lastLogIndex = uint64(len(s.log) - 1)
		}
		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}

				if s.cluster[j].matchIndex >= i {
					quorum--
				}
			}

			if quorum == 0 {
				s.commitIndex = i
				s.debug(fmt.Sprintf("New commit index: %d", i))
				break
			}
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

		// Command == nil is a noop committed by the leader.
		if log.Command != nil {
			// TODO: what if Apply() takes too long?
			res, err := s.statemachine.Apply(log.Command)
			if log.result != nil {
				log.result <- applyResult{
					res: res,
					err: err,
				}
			}
		}

		s.lastApplied++
	}

	s.mu.Unlock()
}

func (s *Server) resetElectionTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.debug(fmt.Sprintf("New interval: %s", interval*time.Millisecond))
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()

	hasTimedOut := time.Now().After(s.electionTimeout)
	if hasTimedOut {
		s.debug("Timed out, starting new election.")
		s.state = candidateState
		s.currentTerm++
		s.votedFor = s.id
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
				s.cluster[i].voted = true
			} else {
				s.cluster[i].votedFor = ""
				s.cluster[i].voted = false
			}
		}
	}
	s.mu.Unlock()

	if hasTimedOut {
		s.resetElectionTimeout()
		s.persist()
		s.requestVote()
	}
}

func (s *Server) becomeLeader() {
	s.mu.Lock()

	quorum := len(s.cluster)/2 + 1
	//s.debug(fmt.Sprintf("Checking for quorum: %d", quorum))
	for i := range s.cluster {
		// Reset all other state while we're at it
		s.cluster[i].nextIndex = uint64(len(s.log) + 1)
		s.cluster[i].matchIndex = 0

		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	if quorum == 0 {
		s.debug("New leader.")
		s.state = leaderState
		s.heartbeatTimeout = time.Now()
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
	}

	s.mu.Unlock()

	if quorum == 0 {
		s.persist()
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()


	do := time.Now().After(s.heartbeatTimeout)
	if do {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
	}

	s.mu.Unlock()

	if do {
		s.appendEntries()
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
		s.resetElectionTimeout()
		ticker := time.NewTicker(time.Millisecond)

		for range ticker.C {
			s.mu.Lock()
			state := s.state
			s.mu.Unlock()

			switch state {
			case leaderState:
				s.heartbeat()
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
