package raft

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

			if rsp.RPCMessage.Term > s.currentTerm {
				s.state = followerState
			}
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
				log.Printf("Server %s is elected leader for term %d", s.id, s.currentTerm)
				s.state = leaderState

				// Reinitialize volatile leader state after every election
				for i := range s.cluster {
					s.cluster[i].nextIndex = s.lastLogIndex + 1
					s.cluster[i].matchIndex = 0
				}

				s.leaderSendHeartbeat()
				return
			}
		}
	}

	if s.state == candidateState {
		// Election failed due to timeout, start a new one
		log.Printf("Server %s is still a candidate but election timed out", s.id)
	}
}

func (s *Server) candidateTick() {
	s.election()
}
