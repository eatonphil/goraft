package goraft

func (s *Server) Id() uint64 {
	return s.id
}

func (s *Server) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == leaderState
}

// Excludes blank entries
func (s *Server) AllCommitted() (bool, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := max(s.commitIndex, 1) - 1; i > 0; i-- {
		e := s.log[i]
		// Last entry in the log that is applied by the user.
		if len(e.Command) > 0 {
			return s.lastApplied >= uint64(i), float64(s.lastApplied) / float64(len(s.log)) * 100
		}
	}

	// Found no user-submitted entries.
	return true, 100
}

type EntriesIterator struct {
	s     *Server
	index int
	Entry Entry
}

func (ei *EntriesIterator) Next() bool {
	ei.s.mu.Lock()
	defer ei.s.mu.Unlock()

	for ei.index < len(ei.s.log) {
		ei.Entry = ei.s.log[ei.index]
		ei.index++
		// Skip ahead until you find the next user-submitted message.
		if len(ei.Entry.Command) > 0 {
			break
		}
	}

	return ei.index == len(ei.s.log)
}

// Exclude blank entries
func (s *Server) AllEntries() *EntriesIterator {
	return &EntriesIterator{
		s:     s,
		index: 0,
		Entry: Entry{},
	}
}
