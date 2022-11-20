package raft

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

func (s *Server) RequestVote(req *RequestVoteRequest, rsp *RequestVoteResponse) error {
	rsp.VoteGranted = false

	done := make(chan bool)
	s.chans.requestVote <- requestVoteMessage{
		message: req,
		callback: func(err error) {
			if err != nil {
				log.Println(err)
			} else {
				rsp.VoteGranted = true
			}
			done <- true
		},
	}
		
	<- done
}

func (s *Server) AppendEntries(req *AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	rsp.Success = false
	rsp.Term = req.Term

	done := make(chan bool)
	s.chans.appendEntries <- appendEntriesMessage{
		message: req,
		callback: func(err error) {
			if err != nil {
				log.Println(err)
			} else {
				rsp.Success = true
				rsp.Term = req.Term
			}
			done <- true
		},
	}

	<- done
}
