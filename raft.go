package goraft

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

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
	electionTimeout time.Duration

	// User-provided state machine
	statemachine StateMachine

	// Metadata directory
	metadataDir string

	// Metadata store
	fd *os.File

	// ----------- VOLATILE STATE -----------

	lastLogIndex uint64

	// Index of highest log entry known to be committed
	commitIndex uint64

	// Index of highest log entry applied to state machine
	lastApplied   uint64
	lastAppliedMu sync.Mutex

	// Candidate, follower, or leader
	state ServerState

	// How long to wait before beginning another election while waiting on the current election
	randomElectionTimeout time.Duration

	// Servers in the cluster, not including this one
	cluster []ClusterMember

	// For keeping the electionTimeout reset
	heartbeat time.Time
}

func NewServer(
	id string,
	address string,
	electionTimeout time.Duration,
	cluster []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
) *Server {
	s := &Server{
		id:              id,
		address:         address,
		electionTimeout: electionTimeout,
		cluster:         cluster,
		statemachine:    statemachine,
		metadataDir:     metadataDir,
		heartbeat:       time.Now(),
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
	if err != nil {
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
	s.lastLogIndex = uint64(len(s.log) - 1)
}

func (s *Server) termChange(msg RPCMessage) bool {
	if msg.Term > s.currentTerm {
		if s.state == followerState {
			log.Printf("Server %s is now a follower", s.id)
		} else {
			log.Printf("Server %s changed from %s to follower", s.id, s.state)
		}
		s.state = followerState
		s.currentTerm = msg.Term
		s.persist()
		return true
	}

	return false
}

func (s *Server) leaderAppendEntries(server ClusterMember, entries []Entry) (*AppendEntriesResponse, error) {
	client, err := rpc.DialHTTP("tcp", server.Address)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to %s at %s: %s", server.Id, server.Address, err)
	}

	prevIndex := server.nextIndex - 1
	req := AppendEntriesRequest{
		RPCMessage: RPCMessage{
			Term: s.currentTerm,
		},
		LeaderId:     s.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  s.log[prevIndex].Term,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	var rsp AppendEntriesResponse
	err = client.Call("Server.AppendEntries", &req, &rsp)
	if err != nil {
		return nil, fmt.Errorf("AppendEntries to %s at %s failed: %s", server.Id, server.Address, err)
	}

	s.termChange(rsp.RPCMessage)

	return &rsp, nil
}

func (s *Server) leaderSendHeartbeat() {
	for _, member := range s.cluster {
		rsp, err := s.leaderAppendEntries(member, nil)
		if err != nil {
			log.Printf("Heartbeat failed: %s", err)
			continue
		}

		if !rsp.Success {
			log.Printf("Heartbeat from leader %s to server %s failed", s.id, member.Id)
		}
	}
}

func (s *Server) leaderElected() {
	// Reinitialize volatile leader state after every election
	for i := range s.cluster {
		s.cluster[i].nextIndex = s.lastLogIndex + 1
		s.cluster[i].matchIndex = 0
	}
	s.leaderSendHeartbeat()
}

func (s *Server) election() {
	// New term and reset to candidate
	s.currentTerm = s.currentTerm + 1
	s.persist()
	s.state = candidateState

	// Wait for last election timeout to finish if necessary
	if s.randomElectionTimeout != 0 {
		<-time.After(s.randomElectionTimeout)
	}

	// If it is not still a candidate (election succeeded), don't keep running the election
	if s.state != candidateState {
		return
	}

	log.Printf("Server %s is starting election for term %d", s.id, s.currentTerm)

	// Random timeout within 150-300ms
	s.randomElectionTimeout = time.Millisecond * time.Duration(rand.Intn(150)+150)

	allServers := len(s.cluster) + 1 /* this Server */
	// Majority needed
	var votesNeeded atomic.Int32
	votesNeeded.Store((int32(math.Floor(float64(allServers) / 2))) + 1)
	// But Servers always vote for themselves
	votesNeeded.Add(-1)

	for _, member := range s.cluster {
		go func(member ClusterMember) {
			client, err := rpc.DialHTTP("tcp", member.Address)
			if err != nil {
				log.Printf("Could not connect to %s at %s: %s", member.Id, member.Address, err)
				return
			}

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: s.lastLogIndex,
				LastLogTerm:  s.log[s.lastLogIndex].Term,
			}
			var rsp RequestVoteResponse
			err = client.Call("Server.RequestVote", req, &rsp)
			if err != nil {
				log.Printf("RequestVote to %s at %s failed: %s", member.Id, member.Address, err)
				return
			}

			if rsp.VoteGranted {
				log.Printf("Server %s voted for %s for term %d", member.Id, s.id, s.currentTerm)
				votesNeeded.Add(-1)
			}

			s.termChange(rsp.RPCMessage)
		}(member)
	}

	// Instantiate this here outside so it doesn't get reset each loop
	timeout := time.After(s.randomElectionTimeout)
outer:
	for {
		select {
		case <-timeout:
			break outer
		default:
			if votesNeeded.Load() <= 0 && s.state == candidateState {
				// Voted for self TODO: is this necessary?
				// s.votedFor = s.id
				s.persist()

				log.Printf("Server %s is elected leader for term %d", s.id, s.currentTerm)
				s.state = leaderState
				s.leaderElected()
				return
			}
		}
	}

	if s.state == candidateState {
		// Election failed due to timeout, start a new one
		log.Printf("Server %s is still a candidate but election timed out", s.id)
		s.election()
	}
}

func (s *Server) RequestVote(req *RequestVoteRequest, rsp *RequestVoteResponse) error {
	rsp.VoteGranted = false
	currentTerm := s.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term <= currentTerm {
		return nil
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (s.votedFor == "" || s.votedFor == req.CandidateId) &&
		// Last log index and last log term are the same
		((req.LastLogIndex == s.lastLogIndex && (req.LastLogIndex == 0 || req.LastLogTerm == s.log[s.lastLogIndex].Term)) ||
			// Or has a later term
			(req.LastLogTerm > currentTerm) ||
			// Or is the same term but has more entries
			(req.LastLogTerm == currentTerm && req.LastLogIndex > s.lastLogIndex)) {
		rsp.VoteGranted = true
		s.votedFor = req.CandidateId
		s.persist()
		return nil
	}

	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func (s *Server) AppendEntries(req *AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	if s.state == leaderState {
		return fmt.Errorf("Leader %s should not be receiving AppendEntries RPC", s.id)
	}

	rsp.Success = false
	if s.state == candidateState {
		if req.Term >= s.currentTerm {
			s.state = followerState
		} else {
			log.Printf("Candidate %s not finding a leader in %s: %d < %d", s.id, req.LeaderId, req.Term, s.currentTerm)
			return nil
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.currentTerm {
		log.Printf("Server %s not finding a leader in %s: %d < %d", s.id, req.LeaderId, req.Term, s.currentTerm)
		return nil
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if s.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		log.Printf("Server %s not finding an up-to-date leader in %s", s.id, req.LeaderId)
		return nil
	}

	s.termChange(req.RPCMessage)

	for i, entry := range req.Entries {
		realIndex := uint64(i) + req.PrevLogIndex + 1
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		if realIndex < s.lastLogIndex && s.log[realIndex].Term != req.Term {
			s.log = s.log[:realIndex]
		}

		// 4. Append any new entries not already in the log
		s.log = append(s.log, entry)
		s.lastLogIndex = uint64(len(s.log) - 1)
	}
	s.persist()

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, s.lastLogIndex)
		log.Printf("Updating server %s to latest commit: %d", s.id, s.commitIndex)
	}

	rsp.Success = true
	s.heartbeat = time.Now()
	log.Printf("Server %s successfully received %d entries from leader %s", s.id, len(req.Entries), req.LeaderId)

	return nil
}

func (s *Server) leaderApplyToFollower(follower ClusterMember) {
	for {
		if s.lastLogIndex >= follower.nextIndex {
			log.Println("How many to update", follower.nextIndex, len(s.log[follower.nextIndex:]))
			rsp, err := s.leaderAppendEntries(follower, s.log[follower.nextIndex:])
			if err != nil {
				log.Printf("Could not connect to %s at %s: %s", follower.Id, follower.Address, err)
				// Need to retry
				continue
			}

			if rsp.Success {
				follower.nextIndex = s.lastLogIndex + 1
				follower.matchIndex = s.lastLogIndex

				log.Printf("Node %s committed", follower.Id)
				return
			} else {
				// At the end, just need to keep retrying
				if follower.nextIndex > 1 {
					follower.nextIndex = follower.nextIndex - 1
				}
				// No break, retry the request
			}
		}
	}
}

func (s *Server) Apply(command []byte) ([]byte, error) {
	if s.state != leaderState {
		return nil, fmt.Errorf("Cannot call append on non-leader")
	}

	currentTerm := s.currentTerm
	s.log = append(s.log, Entry{
		Command: command,
		Term:    currentTerm,
	})
	s.persist()
	s.lastLogIndex = uint64(len(s.log) - 1)

	allServers := len(s.cluster)
	// Majority needed
	majorityNeeded := allServers / 2
	var majorityCounter atomic.Int32
	majorityCounter.Store(int32(majorityNeeded))

	for _, member := range s.cluster {
		go func(member ClusterMember) {
			s.leaderApplyToFollower(member)
			majorityCounter.Add(-1)
		}(member)
	}

	// Wait for a majority to commit
	lastDebug := time.Now()
	for majorityCounter.Load() > 0 {
		if lastDebug.Add(time.Second).Before(time.Now()) {
			lastDebug = time.Now()
			log.Println("Waiting for majority to commit")
		}
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	var minMatchIndex = ^uint64(0) /* max */
	for _, member := range s.cluster {
		matchIndex := member.matchIndex
		if matchIndex >= s.commitIndex &&
			s.log[matchIndex].Term == currentTerm &&
			matchIndex < minMatchIndex {
			minMatchIndex = matchIndex
			majorityNeeded = majorityNeeded - 1
		}
	}
	if majorityNeeded == 0 {
		log.Printf("Leader has new commit index: %d", minMatchIndex)
		s.commitIndex = minMatchIndex
	}

	s.lastAppliedMu.Lock()
	rsp, err := s.statemachine.Apply(command)
	s.lastApplied = s.commitIndex
	s.lastAppliedMu.Unlock()

	return rsp, err
}

func (s *Server) Start() {
	rand.Seed(time.Now().UnixNano())
	var err error
	s.fd, err = os.OpenFile(path.Join(s.metadataDir, s.id), os.O_SYNC|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	s.restore()

	rpc.Register(s)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	go http.Serve(l, nil)

	lastLeaderHeartbeat := time.Now()
	for {
		s.lastAppliedMu.Lock()
		if s.commitIndex > s.lastApplied {
			// lastApplied + 1 since first real entry is at 1, not 0
			_, err := s.statemachine.Apply(s.log[s.lastApplied+1].Command)
			if err != nil {
				panic(err)
			}
			s.lastApplied = s.lastApplied + 1
		}
		s.lastAppliedMu.Unlock()

		if s.state == leaderState {
			if lastLeaderHeartbeat.Add(s.electionTimeout / 2).Before(time.Now()) {
				log.Printf("Leader %s sending heartbeat", s.id)
				lastLeaderHeartbeat = time.Now()
				s.leaderSendHeartbeat()
			}
		} else if s.state == followerState {
			if s.heartbeat.Add(s.electionTimeout).Before(time.Now()) {
				// Got no heartbeat, so start an election
				s.election()
			}
		}
	}
}
