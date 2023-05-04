package goraft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	//sync "github.com/sasha-s/go-deadlock"
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

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Command []byte
	Term    uint64
	result  chan ApplyResult
}

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage

	// Candidate requesting vote
	CandidateId uint64

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
	LeaderId uint64

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
	Id      uint64
	Address string

	// Index of the next log entry to send
	nextIndex uint64
	// Highest log entry known to be replicated
	matchIndex uint64

	votedFor uint64
	voted    bool

	// TCP connection
	rpcClient *rpc.Client
}

type PersistentState struct {
	// The current term
	CurrentTerm uint64

	// candidateId that received vote in current term (or null if none)
	VotedFor uint64

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
	votedFor uint64

	log []Entry

	// ----------- READONLY STATE -----------

	// Unique identifier for this Server
	id uint64

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
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}

	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...any) {
	fmt.Println(fmt.Sprintf(msg, args...))
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
	//sync.Opts.DeadlockTimeout = 50 * time.Millisecond
	// Explicitly make a copy of the cluster because we'll be
	// modifying it in this server.
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
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

const PAGE_SIZE = 4096
const ENTRY_HEADER = 16
const ENTRY_SIZE = PAGE_SIZE

// Weird thing to note is that writing to a deleted disk is not an
// error on Linux. So if these files are deleted, you won't know about
// that until the process restarts.
func (s *Server) persist(writeLog bool, nNewEntries int) {
	t := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}

	s.fd.Seek(0, 0)

	var page [PAGE_SIZE]byte
	// Bytes 0  - 8:   Current term
	// Bytes 8  - 16:  Voted for
	// Bytes 16 - 24:  Log length
	// Bytes 4096 - N: Log

	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.votedFor)
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))
	n, err := s.fd.Write(page[:])
	if err != nil {
		panic(err)
	}
	Server_assert(s, "Wrote full page", n, PAGE_SIZE)

	if writeLog && nNewEntries > 0 {
		newLogOffset := max(len(s.log)-nNewEntries, 0)

		s.fd.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.fd)

		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			// Bytes 0 - 8:    Entry term
			// Bytes 8 - 16:   Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command

			if len(s.log[i].Command) > ENTRY_SIZE-ENTRY_HEADER {
				panic(fmt.Sprintf("Command is too large. Must be at most %d bytes.", ENTRY_SIZE-ENTRY_HEADER))
			}

			binary.LittleEndian.PutUint64(entryBytes[:8], s.log[i].Term)
			binary.LittleEndian.PutUint64(entryBytes[8:16], uint64(len(s.log[i].Command)))
			copy(entryBytes[16:], []byte(s.log[i].Command))

			n, err := bw.Write(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Wrote full page", n, ENTRY_SIZE)
		}

		err = bw.Flush()
		if err != nil {
			panic(err)
		}
	}

	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("Persisted in %s. Term: %d. Log Len: %d (%d new). Voted For: %d.", time.Now().Sub(t), s.currentTerm, len(s.log), nNewEntries, s.votedFor)
}

func (s *Server) initialize() {
	if len(s.log) == 0 {
		// Always has at least one log entry.
		s.log = []Entry{{}}
	}

	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = s.votedFor
		}
	}
}

func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755)
		if err != nil {
			panic(err)
		}
	}

	s.fd.Seek(0, 0)

	// Bytes 0  - 8:   Current term
	// Bytes 8  - 16:  Voted for
	// Bytes 16 - 24:  Log length
	// Bytes 4096 - N: Log
	var page [PAGE_SIZE]byte
	n, err := s.fd.Read(page[:])
	if err == io.EOF {
		s.initialize()
		return
	} else if err != nil {
		panic(err)
	}
	Server_assert(s, "Read full page", n, PAGE_SIZE)

	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.votedFor = binary.LittleEndian.Uint64(page[8:16])
	lenLog := binary.LittleEndian.Uint64(page[16:24])

	if lenLog > 0 {
		s.fd.Seek(int64(PAGE_SIZE), 0)

		s.log = make([]Entry, lenLog)
		var e Entry
		for i := 0; i < len(s.log); i++ {
			var entryBytes [ENTRY_SIZE]byte
			n, err := s.fd.Read(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Read full entry", n, ENTRY_SIZE)

			// Bytes 0 - 8:    Entry term
			// Bytes 8 - 16:   Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command
			e.Term = binary.LittleEndian.Uint64(entryBytes[:8])
			lenValue := binary.LittleEndian.Uint64(entryBytes[8:16])
			e.Command = entryBytes[16 : 16+lenValue]
			s.log[i] = e
		}
	}

	s.initialize()
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
			s.debugf("Requesting vote from %d.", s.cluster[i].Id)
			s.cluster[i].voted = true

			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[len(s.log)-1].Term

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
				s.debugf("Vote granted by %d.", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.updateTerm(req.RPCMessage)

	s.mu.Lock()
	s.debugf("Received vote request from %d.", req.CandidateId)

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	var grant bool
	if req.Term < s.currentTerm {
		s.debugf("Not granting vote request from %d.", req.CandidateId)
		Server_assert(s, "VoteGranted = false", rsp.VoteGranted, false)
	} else {
		lastLogTerm := s.log[len(s.log)-1].Term
		logLen := uint64(len(s.log) - 1)
		logOk := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)
		grant = req.Term == s.currentTerm &&
			logOk &&
			(s.votedFor == 0 || s.votedFor == req.CandidateId)
		if grant {
			s.debugf("Voted for %d.", req.CandidateId)
			s.votedFor = req.CandidateId
			rsp.VoteGranted = true
		} else {
			s.debugf("Not granting vote request from %d.", +req.CandidateId)
		}
	}

	s.mu.Unlock()

	if grant {
		s.resetElectionTimeout()
		s.persist(false, 0)
	}

	return nil
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	s.mu.Lock()

	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.votedFor = 0
		transitioned = true
		s.debug("Transitioned to follower")
	}
	s.mu.Unlock()

	if transitioned {
		s.resetElectionTimeout()
		s.persist(false, 0)
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
		s.debugf("Dropping request from old leader %d: term %d.", req.LeaderId, req.Term)
		s.mu.Unlock()
		// Not a valid leader.
		return nil
	}

	s.mu.Unlock()

	// Valid leader so reset election.
	s.resetElectionTimeout()

	s.mu.Lock()

	logLen := uint64(len(s.log))
	validPreviousLog := req.PrevLogIndex == 0 /* This is the induction step */ ||
		(req.PrevLogIndex < logLen &&
			s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.debug("Not a valid log.")
		s.mu.Unlock()
		return nil
	}

	next := req.PrevLogIndex + 1
	nNewEntries := 0
	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]
		if i >= uint64(len(s.log)) || s.log[i].Term != e.Term {
			// Either allocate space for the whole thing, or truncate when terms mismatch.
			newLog := make([]Entry, next+uint64(len(req.Entries)))
			copy(newLog, s.log)
			s.log = newLog
		}

		s.log[i] = e
		nNewEntries++
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.mu.Unlock()
	s.persist(nNewEntries != 0, nNewEntries)

	rsp.Success = true
	return nil
}

var ErrApplyToLeader = errors.New("Cannot apply message to follower, apply to leader.")

var lastApply = time.Now()

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}
	s.debug("Processing new entry!")

	resultChans := make([]chan ApplyResult, len(commands))
	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
	}
	s.mu.Unlock()
	s.persist(true, 1)

	go func () {
		delay := 3*time.Millisecond
		time.Sleep(delay)
		s.mu.Lock()
		if time.Now().Sub(lastApply) > delay {
			s.appendEntries()
			lastApply = time.Now()
		}
		s.mu.Unlock()
	}()

	// TODO: What happens if this takes too long?
	s.debug("Waiting to be applied!")

	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))
	for i, ch := range resultChans {
		go func(i int, c chan ApplyResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}

	wg.Wait()

	return results, nil
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
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}

	return err == nil
}

const MAX_APPEND_ENTRIES_BATCH = 8_000

func (s *Server) appendEntries() {
	for i := range s.cluster {
		// Don't need to send message to self
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term

			var entries []Entry
			if uint64(len(s.log)-1) >= s.cluster[i].nextIndex {
				s.debugf("len: %d, next: %d, server: %d", len(s.log), next, s.cluster[i].Id)
				entries = s.log[next:]
			}

			// Keep latency down by only applying N at a time.
			if len(entries) > MAX_APPEND_ENTRIES_BATCH {
				entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			}

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
			s.debugf("Sending %d entries to %d for term %d.", len(entries), s.cluster[i].Id, req.Term)
			ok := s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp)
			if !ok {
				// Will retry next tick
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
				s.cluster[i].nextIndex = max(req.PrevLogIndex+lenEntries+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				s.debugf("Message accepted for %d. Prev Index: %d, Next Index: %d, Match Index: %d.", s.cluster[i].Id, prev, s.cluster[i].nextIndex, s.cluster[i].matchIndex)
			} else {
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
				s.debugf("Forced to go back to %d for: %d.", s.cluster[i].nextIndex, s.cluster[i].Id)
			}
		}(i)
	}
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Leader can update commitIndex on quorum.
	if s.state == leaderState {
		lastLogIndex := uint64(len(s.log) - 1)

		for i := lastLogIndex; i > s.commitIndex; i-- {
			// not `len(s.cluster)/2 + 1` since the leader already has the entry.
			quorum := len(s.cluster) / 2
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
				s.debugf("New commit index: %d.", i)
				break
			}
		}
	}

	if s.lastApplied <= s.commitIndex {
		if s.lastApplied == 0 {
			// First entry is always a blank one.
			s.lastApplied++
			return
		}

		log := s.log[s.lastApplied]

		// Command == nil is a noop committed by the leader.
		if log.Command != nil {
			s.debugf("Entry applied: %d.", s.lastApplied)
			// TODO: what if Apply() takes too long?
			res, err := s.statemachine.Apply(log.Command)

			// Will be nil for follower entries and for no-op entries.
			// Not nil for all user submitted messages.
			if log.result != nil {
				log.result <- ApplyResult{
					Result: res,
					Error:  err,
				}
			}
		}

		s.lastApplied++
	}
}

func (s *Server) resetElectionTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.debugf("New interval: %s.", interval*time.Millisecond)
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
				s.cluster[i].votedFor = 0
				s.cluster[i].voted = false
			}
		}
	}
	s.mu.Unlock()

	if hasTimedOut {
		s.resetElectionTimeout()
		s.persist(false, 0)
		s.requestVote()
	}
}

func (s *Server) becomeLeader() {
	s.mu.Lock()

	quorum := len(s.cluster)/2 + 1
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
		s.persist(true, 1)
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()

	timeForHeartbeat := time.Now().After(s.heartbeatTimeout)
	if timeForHeartbeat {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
	}

	s.mu.Unlock()

	if timeForHeartbeat {
		s.appendEntries()
	}
}

// Make sure rand is seeded
func (s *Server) Start() {
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
				s.advanceCommitIndex()
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

// Excludes heartbeat entries
func (s *Server) AllCommitted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := len(s.log)-1; i >= 0; i-- {
		e := s.log[i]
		// Last entry in the log that is applied by the user.
		if len(e.Command) > 0 {
			return s.lastApplied >= uint64(i)
		}
	}

	return true
}
