package main

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

type ClusterEntry struct {
	Id      string
	Address string
	// Index of the next log entry to send
	nextIndex uint64
	// Highest log entry known to be replicated
	matchIndex uint64
}

type ServerPersistent struct {
	// The current term
	CurrentTerm uint64

	// candidateId that received vote in current term (or null if none)
	VotedFor Optional[string]

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

	Persistent ServerPersistent

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
	lastApplied uint64

	// Candidate, follower, or leader
	state ServerState

	// How long to wait before beginning another election while waiting on the current election
	randomElectionTimeout time.Duration

	// Servers in the cluster, not including this one
	cluster []ClusterEntry

	// For keeping the electionTimeout reset
	heartbeat time.Time
}

func newServer(
	id string,
	address string,
	electionTimeout time.Duration,
	cluster []ClusterEntry,
	statemachine StateMachine,
	metadataDir string,
) *Server {
	return &Server{
		state:           followerState,
		id:              id,
		address:         address,
		electionTimeout: electionTimeout,
		cluster:         cluster,
		statemachine:    statemachine,
		metadataDir:     metadataDir,
		heartbeat:       time.Now(),
	}
}

func (s *Server) persist() {
	s.fd.Truncate(0)
	s.fd.Seek(0, 0)
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(s.Persistent)
	if err != nil {
		panic(err)
	}
}

func (s *Server) restore() {
	s.fd.Seek(0, 0)
	dec := gob.NewDecoder(s.fd)
	err := dec.Decode(&s.Persistent)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if len(s.Persistent.Log) > 0 {
		s.lastLogIndex = uint64(len(s.Persistent.Log) - 1)
	}
}

func (s *Server) termChange(msg RPCMessage) bool {
	if msg.Term > s.Persistent.CurrentTerm {
		if s.state == followerState {
			log.Printf("Server %s is now a follower", s.id)
		} else {
			log.Printf("Server %s changed from %s to follower", s.id, s.state)
		}
		s.state = followerState
		s.Persistent.CurrentTerm = msg.Term
		s.persist()
		return true
	}

	return false
}

func (s *Server) leaderAppendEntries(server ClusterEntry, entries []Entry) (*AppendEntriesResponse, error) {
	client, err := rpc.DialHTTP("tcp", server.Address)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to %s at %s: %s", server.Id, server.Address, err)
	}

	var prevLogTerm, prevLogIndex uint64
	if server.nextIndex > 0 {
		prevLogTerm = s.Persistent.Log[server.nextIndex-1].Term
		prevLogIndex = server.nextIndex - 1
	}

	req := AppendEntriesRequest{
		RPCMessage: RPCMessage{
			Term: s.Persistent.CurrentTerm,
		},
		LeaderId:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
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
	for _, other := range s.cluster {
		rsp, err := s.leaderAppendEntries(other, nil)
		if err != nil {
			log.Printf("Heartbeat failed: %s", err)
			continue
		}

		if !rsp.Success {
			log.Printf("Heartbeat from leader %s to server %s failed", s.id, other.Id)
		}
	}
}

func (s *Server) leaderElected() {
	// Reinitialize volatile leader state after every election
	for _, others := range s.cluster {
		others.nextIndex = s.lastLogIndex + 1
		others.matchIndex = 0
	}
	s.leaderSendHeartbeat()
}

func (s *Server) election() {
	// New term and reset to candidate
	s.Persistent.CurrentTerm++
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

	log.Printf("Server %s is starting election for term %d", s.id, s.Persistent.CurrentTerm)

	// Random timeout within 150-300ms
	s.randomElectionTimeout = time.Millisecond * time.Duration(rand.Intn(150)+150)

	allServers := len(s.cluster) + 1 /* this Server */
	// Majority needed
	var votesNeeded atomic.Int32
	votesNeeded.Store((int32(math.Floor(float64(allServers) / 2))) + 1)
	// But Servers always vote for themselves
	votesNeeded.Add(-1)

	for _, other := range s.cluster {
		go func(other ClusterEntry) {
			client, err := rpc.DialHTTP("tcp", other.Address)
			if err != nil {
				log.Printf("Could not connect to %s at %s: %s", other.Id, other.Address, err)
				return
			}

			// Special case for when there are zero logs so far
			var lastLogTerm uint64
			lastLogIndex := s.lastLogIndex
			if len(s.Persistent.Log) == 0 {
				lastLogIndex = 0
			} else {
				lastLogTerm = s.Persistent.Log[lastLogIndex].Term
			}

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.Persistent.CurrentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var rsp RequestVoteResponse
			err = client.Call("Server.RequestVote", req, &rsp)
			if err != nil {
				log.Printf("RequestVote to %s at %s failed: %s", other.Id, other.Address, err)
				return
			}

			if rsp.VoteGranted {
				log.Printf("Server %s voted for %s for term %d", other.Id, s.id, s.Persistent.CurrentTerm)
				votesNeeded.Add(-1)
			}

			s.termChange(rsp.RPCMessage)
		}(other)
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
				// Voted for self
				s.Persistent.VotedFor.Put(s.id)
				s.persist()

				log.Printf("Server %s is elected leader for term %d", s.id, s.Persistent.CurrentTerm)
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

	// 1. Reply false if term < Persistent.CurrentTerm (§5.1)
	if req.Term <= s.Persistent.CurrentTerm {
		return nil
	}

	// If Persistent.VotedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (!s.Persistent.VotedFor.Set() || s.Persistent.VotedFor.Value() == req.CandidateId) &&
		// Last log index and last log term are the same
		((req.LastLogIndex == s.lastLogIndex && (req.LastLogIndex == 0 || req.LastLogTerm == s.Persistent.Log[s.lastLogIndex].Term)) ||
			// Or has a later term
			(req.LastLogTerm > s.Persistent.CurrentTerm) ||
			// Or is the same term but has more entries
			(req.LastLogTerm == s.Persistent.CurrentTerm && req.LastLogIndex > s.lastLogIndex)) {
		rsp.VoteGranted = true
		s.Persistent.VotedFor.Put(req.CandidateId)
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
		if req.Term >= s.Persistent.CurrentTerm {
			s.state = followerState
		} else {
			log.Printf("Candidate %s not finding a leader in %s: %d < %d", s.id, req.LeaderId, req.Term, s.Persistent.CurrentTerm)
			return nil
		}
	}

	// 1. Reply false if term < Persistent.CurrentTerm (§5.1)
	if req.Term < s.Persistent.CurrentTerm {
		log.Printf("Candidate %s not finding a leader in %s: %d < %d", s.id, req.LeaderId, req.Term, s.Persistent.CurrentTerm)
		return nil
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if !(req.PrevLogIndex == 0 && s.lastLogIndex == 0 && req.PrevLogTerm == 0) && (req.PrevLogIndex >= s.lastLogIndex ||
		s.Persistent.Log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		log.Printf("Candidate %s not finding an up-to-date leader in %s", s.id, req.LeaderId)
		return nil
	}

	s.termChange(req.RPCMessage)

	for i, entry := range req.Entries {
		realIndex := uint64(i) + req.PrevLogIndex + 1
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		if realIndex < s.lastLogIndex && s.Persistent.Log[realIndex].Term != req.Term {
			s.Persistent.Log = s.Persistent.Log[:realIndex]
		}

		// 4. Append any new entries not already in the log
		s.Persistent.Log = append(s.Persistent.Log, entry)
		s.lastLogIndex++
	}
	s.persist()

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, s.lastLogIndex)
	}

	rsp.Success = true
	s.heartbeat = time.Now()
	log.Printf("Server %s successfully received %d entries from leader %s", s.id, len(req.Entries), req.LeaderId)

	return nil
}

func (s *Server) Apply(command []byte) ([]byte, error) {
	if s.state != leaderState {
		return nil, fmt.Errorf("Cannot call append on non-leader")
	}

	s.Persistent.Log = append(s.Persistent.Log, Entry{
		Command: command,
		Term:    s.Persistent.CurrentTerm,
	})
	s.persist()
	s.lastLogIndex = uint64(len(s.Persistent.Log) - 1)

	allServers := len(s.cluster)
	// Majority needed
	majorityNeeded := allServers / 2
	var majorityCounter atomic.Int32
	majorityCounter.Store(int32(majorityNeeded))

	for _, other := range s.cluster {
		go func(other ClusterEntry) {
			if s.lastLogIndex >= other.nextIndex {
				rsp, err := s.leaderAppendEntries(other, s.Persistent.Log[other.nextIndex:])
				if err != nil {
					log.Printf("Could not connect to %s at %s: %s", other.Id, other.Address, err)
					return
				}

				if rsp.Success {
					other.nextIndex = s.lastLogIndex + 1
					other.matchIndex = s.lastLogIndex

					log.Printf("Node %s committed", other.Id)
					majorityCounter.Add(-1)
					return
				} else {
					other.nextIndex--
					// No break, retry the request
				}
			}
		}(other)
	}

	// Wait for a majority to commit
	for majorityCounter.Load() > 0 {
		log.Println("Waiting for majority to commit")
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == Persistent.CurrentTerm:
	// set commitIndex = N (§5.3, §5.4).
	var minMatchIndex = ^uint64(0) /* max */
	for _, other := range s.cluster {
		if other.matchIndex >= s.commitIndex &&
			s.Persistent.Log[other.matchIndex].Term == s.Persistent.CurrentTerm &&
			other.matchIndex < minMatchIndex {
			minMatchIndex = other.matchIndex
			majorityNeeded--
		}
	}
	if majorityNeeded == 0 {
		s.commitIndex = minMatchIndex
	}

	rsp, err := s.statemachine.Apply(command)
	s.lastApplied = s.commitIndex

	return rsp, err
}

func (s *Server) start() {
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
		if s.commitIndex > s.lastApplied {
			s.statemachine.Apply(s.Persistent.Log[s.lastApplied].Command)
			s.lastApplied++
		}

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
