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
	sync.Opts.DeadlockTimeout = 2000 * time.Millisecond
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

// Weird thing to note is that writing to a deleted disk is not an
// error on Linux. So if these files are deleted, you won't know about
// that until the process restarts.
func (s *Server) persist(writeLogs bool, nNewLogs int) {
	t := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if nNewLogs == 0 && writeLogs {
		nNewLogs = len(s.log)
	}

	s.fd.Seek(0, 0)

	bw := bufio.NewWriter(s.fd)
	// Bytes 0  - 8:  Current term
	// Bytes 8  - 24: Voted for
	// Bytes 24 - 32: Log length
	// Bytes 32 - N:  Log
	err := binary.Write(bw, binary.LittleEndian, s.currentTerm)
	if err != nil {
		panic(err)
	}

	err = binary.Write(bw, binary.LittleEndian, s.votedFor)
	if err != nil {
		panic(err)
	}

	// Cast is important, want to write uint64 bytes.
	err = binary.Write(bw, binary.LittleEndian, uint64(len(s.log)))
	if err != nil {
		panic(err)
	}

	if nNewLogs != len(s.log) {
		// Flush because we're about to seek.
		err := bw.Flush()
		if err != nil {
			panic(err)
		}
	}

	newLogOffset := len(s.log) - nNewLogs

	s.fd.Seek(int64(32+256*newLogOffset), 0)
	bw = bufio.NewWriter(s.fd)

	for i := newLogOffset; i < len(s.log); i++ {
		// Bytes 0 - 8:    Entry term
		// Bytes 8 - 16:   Entry command length
		// Bytes 16 - 256: Entry command
		err = binary.Write(bw, binary.LittleEndian, s.log[i].Term)
		if err != nil {
			panic(err)
		}

		if len(s.log[i].Command) > 240 {
			panic("Command is too large. Must be at most 240 bytes.")
		}

		err = binary.Write(bw, binary.LittleEndian, uint64(len(s.log[i].Command)))
		if err != nil {
			panic(err)
		}

		written := 0
		for written != len(s.log[i].Command) {
			n, err := bw.Write(s.log[i].Command)
			if err != nil {
				panic(err)
			}
			written += n
		}

		// Pad out to 240 bytes.
		for written < 240 {
			written++
			err := bw.WriteByte(0)
			if err != nil {
				panic(err)
			}
		}
	}

	if err = bw.Flush(); err != nil {
		panic(err)
	}

	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("Persisted in %s. Term: %d. Log Len: %d (%d new). Voted For: %d.", time.Now().Sub(t), s.currentTerm, len(s.log), nNewLogs, s.votedFor)
}

func (s *Server) restore() {
	s.fd.Seek(0, 0)

	br := bufio.NewReader(s.fd)
	// Bytes 0  - 8:  Current term
	// Bytes 8  - 24: Voted for
	// Bytes 24 - 32: Log length
	// Bytes 32 - N:  Log
	err := binary.Read(br, binary.LittleEndian, &s.currentTerm)
	// File hasn't been written yet.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return
	}
	if err != nil {
		panic(err)
	}

	err = binary.Read(br, binary.LittleEndian, &s.votedFor)
	if err != nil {
		panic(err)
	}

	// Cast is important, want to write uint64 bytes.
	var lenLog uint64
	err = binary.Read(br, binary.LittleEndian, &lenLog)
	if err != nil {
		panic(err)
	}

	s.log = make([]Entry, lenLog)
	var e Entry
	for i := 0; i < len(s.log); i++ {
		// Bytes 0 - 8:    Entry term
		// Bytes 8 - 16:   Entry command length
		// Bytes 16 - 256: Entry command
		err = binary.Read(br, binary.LittleEndian, &e.Term)
		if err != nil {
			panic(err)
		}

		var len uint64
		err = binary.Read(br, binary.LittleEndian, &len)
		if err != nil {
			panic(err)
		}

		var slot [240]byte
		for i := 0; i < 240; i++ {
			b, err := br.ReadByte()
			if err != nil {
				panic(err)
			}
			slot[i] = b
		}

		// Command may be smaller than slot size.
		e.Command = slot[:len]

		s.log[i] = e
	}

	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = s.votedFor
		}
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
			s.debugf("Requesting vote from %d.", s.cluster[i].Id)
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
		var lastLogTerm, logLen uint64
		if len(s.log) > 0 {
			lastLogTerm = s.log[len(s.log)-1].Term
			logLen = uint64(len(s.log) - 1)
		}
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
	validPreviousLog := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex < logLen &&
			s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.debug("Not a valid log.")
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
	Assert("Log contains new entries (if any)", len(s.log), int(logLen)+len(req.Entries))
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.mu.Unlock()
	s.persist(len(req.Entries) != 0, len(req.Entries))

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
	s.persist(true, 1)

	s.appendEntries(false)

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
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}

	return err == nil
}

func (s *Server) appendEntries(heartbeat bool) {
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
			// TODO: this is wrong. Sometimes it sends all entries even though that isn't necessary.
			if s.cluster[i].nextIndex > lenLog-1 {
				prevLogIndex = s.cluster[i].nextIndex - 1
				prevLogTerm = s.log[prevLogIndex].Term
				logBegin = prevLogIndex + 1
			}

			entries = s.log[logBegin:]
			// Keep latency down by only applying N at a time.
			if len(entries) > 50 {
				entries = entries[:50]
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
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				s.debugf("Message accepted for %d. Prev Index: %d Next Index: %d.", s.cluster[i].Id, prev, s.cluster[i].nextIndex)
			} else {
				s.cluster[i].nextIndex = min(s.cluster[i].nextIndex, 1)
				s.debugf("Forced to go back to %d for: %d.", s.cluster[i].nextIndex, s.cluster[i].Id)
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

	for s.lastApplied <= s.commitIndex {
		if s.lastApplied == 0 {
			// First entry is always a blank one.
			s.lastApplied++
			continue
		}

		log := s.log[s.lastApplied]

		// Command == nil is a noop committed by the leader.
		if log.Command != nil {
			s.debugf("Entry applied: %d.", s.lastApplied)
			// TODO: what if Apply() takes too long?
			res, err := s.statemachine.Apply(log.Command)

			// Will be nil for follower logs and for no-op logs.
			// Not nil for all user submitted messages.
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

	do := time.Now().After(s.heartbeatTimeout)
	if do {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
	}

	s.mu.Unlock()

	if do {
		s.appendEntries(true)
	}
}

// Make sure rand is seeded
func (s *Server) Start() {
	var err error
	s.fd, err = os.OpenFile(
		path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
		os.O_SYNC|os.O_CREATE|os.O_RDWR,
		0755)
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

func (s *Server) Entries() int {
	s.mu.Lock()
	e := len(s.log)
	s.mu.Unlock()
	return e
}
