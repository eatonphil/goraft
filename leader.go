package raft

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

	if rsp.RPCMessage.Term > s.currentTerm {
		s.state = followerState
	}

	return &rsp, nil
}

func (s *Server) leaderApplyToFollower(follower ClusterMember) {
	for s.state == leaderState {
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

func (s *Server) leaderTick() {
	if s.heartbeat.Add(s.electionTimeout / 2).Before(time.Now()) {
		log.Printf("Leader %s sending heartbeat", s.id)
		s.heartbeat = time.Now()
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
}
