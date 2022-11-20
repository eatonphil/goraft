package raft


func (s *Server) followerRequestVote() {
	currentTerm := s.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term <= currentTerm {
		return nil
	}

	if req.Term > currentTerm {
		s.state = followerState
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

func (s *Server) followerAppendEntries(appendEntriesMessage) error {
	if req.Term > s.currentTerm {
		s.state = followerState
	}

	if s.state == leaderState {
		return fmt.Errorf("Leader %s should not be receiving AppendEntries RPC", s.id)
	}

	if s.state == candidateState {
		log.Printf("Candidate %s not finding a leader in %s: %d < %d", s.id, req.LeaderId, req.Term, s.currentTerm)
		return nil
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

	s.heartbeat = time.Now()
	log.Printf("Server %s successfully received %d entries from leader %s", s.id, len(req.Entries), req.LeaderId)

	return nil
}

func (s *Server) followerTick() {
	if s.heartbeat.Add(s.electionTimeout).Before(time.Now()) {
		// Got no heartbeat, so start an election
		s.state = candidateState
		return
	}

	select {
	case m <- s.chans.appendEntries:
		err := s.followerAppendEntries(m.message)
		m.callback(err)
	case m <- s.chans.requestVote:
		err := s.followerRequestVote(m.message)
		m.callback(err)
	}
}
