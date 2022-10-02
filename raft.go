package main

import (
	"encoding/gob"
	"fmt"
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

type ServerState uint

const (
	leaderState ServerState = iota
	followerState
	candidateState
)

type Server struct {
	// ----------- PERSISTENT STATE -----------

	// The current term
	currentTerm uint64

	// candidateId that received vote in current term (or null if none)
	votedFor Optional[string]

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
		currentTerm:     0,
		electionTimeout: electionTimeout,
		cluster:         cluster,
		statemachine:    statemachine,
		metadataDir:     metadataDir,
	}
}

func (s *Server) persist() {
	s.fd.Truncate(0)
	s.fd.Seek(0, 0)
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(struct {
		CurrentTerm uint64
		VotedFor    Optional[string]
		Log         []Entry
	}{
		s.currentTerm,
		s.votedFor,
		s.log,
	})
	if err != nil {
		panic(err)
	}
}

func (s *Server) restore() {
	s.fd.Seek(0, 0)
	dec := gob.NewDecoder(s.fd)
	err := dec.Decode(struct {
		CurrentTerm uint64
		VotedFor    Optional[string]
		Log         []Entry
	}{
		s.currentTerm,
		s.votedFor,
		s.log,
	})
	// TODO: this will probably panic when the file is first created
	if err != nil {
		panic(err)
	}
	s.lastLogIndex = uint64(len(s.log) - 1)
}

func (s *Server) termChange(msg RPCMessage) bool {
	if msg.Term > s.currentTerm {
		s.state = followerState
		s.currentTerm = msg.Term
		s.persist()
		return true
	}

	return false
}

func (s *Server) sendHeartbeat() {
	for _, other := range s.cluster {
		client, err := rpc.DialHTTP("tcp", other.Address)
		if err != nil {
			log.Printf("Could not connect to %s at %s: %s", other.Id, other.Address, err)
			return
		}

		var req AppendEntriesRequest
		var rsp AppendEntriesResponse
		err = client.Call("Server.AppendEntries", &req, &rsp)
		if err != nil {
			log.Printf("AppendEntries to %s at %s failed: %s", other.Id, other.Address, err)
			return
		}

		s.termChange(rsp.RPCMessage)
	}
}

func (s *Server) elected() {
	log.Println("%s is elected leader for term %d", s.id, s.currentTerm)
	s.state = leaderState
	// Reinitialize volatile leader state after every election
	for _, others := range s.cluster {
		others.nextIndex = s.lastLogIndex + 1
		others.matchIndex = 0
	}
	s.sendHeartbeat()
}

func (s *Server) election() {
	// Wait for last election timeout to finish if necessary
	if s.randomElectionTimeout != 0 {
		<-time.After(s.randomElectionTimeout)
	}

	// New term and reset to candidate
	s.currentTerm++
	s.state = candidateState

	log.Println("%s is starting election for term %d", s.id, s.currentTerm)

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
				log.Printf("RequestVote to %s at %s failed: %s", other.Id, other.Address, err)
				return
			}

			if rsp.VoteGranted {
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
			if votesNeeded.Load() == 0 && s.state == candidateState {
				s.elected()
				return
			}
		}
	}

	// Election failed due to timeout, start a new one
	s.election()
}

func (s *Server) RequestVote(req *RequestVoteRequest, rsp *RequestVoteResponse) error {
	rsp.VoteGranted = false

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term <= s.currentTerm {
		return nil
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (!s.votedFor.Set() || s.votedFor.Value() == req.CandidateId) &&
		// Last log index and last log term are the same
		((req.LastLogIndex == s.lastLogIndex && req.LastLogTerm == s.log[s.lastLogIndex].Term) ||
			// Or has a later term
			(req.LastLogTerm > s.currentTerm) ||
			// Or is the same term but has more entries
			(req.LastLogTerm == s.currentTerm && req.LastLogIndex > s.lastLogIndex)) {
		rsp.VoteGranted = true
		s.votedFor.Put(req.CandidateId)
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
			return nil
		}
	}

	s.heartbeat = time.Now()

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.currentTerm {
		return nil
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex >= s.lastLogIndex || s.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return nil
	}

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
		s.lastLogIndex++
	}
	s.persist()

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, s.lastLogIndex)
	}

	return nil
}

func (s *Server) Apply(command []byte) ([]byte, error) {
	if s.state != leaderState {
		return nil, fmt.Errorf("Cannot call append on non-leader")
	}

	s.log = append(s.log, Entry{
		Command: command,
		Term:    s.currentTerm,
	})
	s.persist()
	s.lastLogIndex = uint64(len(s.log) - 1)

	allServers := len(s.cluster)
	// Majority needed
	majorityNeeded := allServers / 2
	var majorityCounter atomic.Int32
	majorityCounter.Store(int32(majorityNeeded))

	for _, other := range s.cluster {
		go func(other ClusterEntry) {
			if s.lastLogIndex >= other.nextIndex {
				client, err := rpc.DialHTTP("tcp", other.Address)
				if err != nil {
					log.Printf("Could not connect to %s at %s: %s", other.Id, other.Address, err)
					return
				}

				req := AppendEntriesRequest{
					RPCMessage: RPCMessage{
						Term: s.currentTerm,
					},
					LeaderId:     s.id,
					PrevLogIndex: other.nextIndex - 1,
					PrevLogTerm:  s.log[other.nextIndex-1].Term,
					Entries:      s.log[other.nextIndex:],
					LeaderCommit: s.commitIndex,
				}
				var rsp AppendEntriesResponse
				err = client.Call("Server.AppendEntries", &req, &rsp)
				if err != nil {
					log.Printf("AppendEntries to %s at %s failed: %s", other.Id, other.Address, err)
					return
				}

				if s.termChange(rsp.RPCMessage) {
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
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	var minMatchIndex = ^uint64(0) /* max */
	for _, other := range s.cluster {
		if other.matchIndex >= s.commitIndex && s.log[other.matchIndex].Term == s.currentTerm && other.matchIndex < minMatchIndex {
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
			s.statemachine.Apply(s.log[s.lastApplied].Command)
			s.lastApplied++
		}

		if s.state == leaderState {
			if lastLeaderHeartbeat.Add(s.electionTimeout / 2).Before(time.Now()) {
				lastLeaderHeartbeat = time.Now()
				s.sendHeartbeat()
			}
		} else if s.state == followerState {
			if s.heartbeat.Add(s.electionTimeout).Before(time.Now()) {
				// Got no heartbeat, so start an election
				s.election()
			}
		}
	}
}
