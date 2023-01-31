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
	//"sync"
	sync "github.com/sasha-s/go-deadlock"
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

type appendEntriesMessage struct {
	message AppendEntriesRequest
	callback func (error)
}

type requestVoteMessage struct {
	message AppendEntriesRequest
	callback func (error)
}

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
	chans struct {
		appendEntries chan appendEntriesMessage
		requestVote chan requestVoteMessage
		apply chan applyMessage
	}

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

	rsp, err := s.statemachine.Apply(command)
	s.lastApplied = s.commitIndex

	return rsp, err
}

func (s *Server) tick(mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()

	if s.commitIndex > s.lastApplied {
		// lastApplied + 1 since first real entry is at 1, not 0
		_, err := s.statemachine.Apply(s.log[s.lastApplied+1].Command)
		if err != nil {
			panic(err)
		}
		s.lastApplied = s.lastApplied + 1
	}

	if s.state == leaderState {
		s.leaderTick()
	} else if s.state == followerState {
		s.followerTick()
	} else if s.state == candidateState {
		s.candidateTick()
	}
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

	var mu sync.Mutex
	for {
		s.tick(&mu)
	}
}
