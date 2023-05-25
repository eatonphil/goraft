// This file contains methods that are mainly used by
// cmd/stress/main.go. Eventually they should be wrapped in a separate
// struct so these methods don't appear as methods on the main Server
// struct.

package goraft

import (
	"context"
)

func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.debug("Shutting down.")
	s.fd.Close()
	s.fd = nil
	s.server.Shutdown(context.Background())
	s.done = true
}

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

func (ei *EntriesIterator) Next() (int, bool) {
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

	// Can't just compare `ei.index == len(s.log)` because in the
	// case where there are blank messages in the end of the log
	// after non-blank messages, Next() wouldn't otherwise know
	// there is not more. So we peek here.
	hasMore := false
	for i := ei.index; i < len(ei.s.log); i++ {
		if len(ei.s.log[i].Command) > 0 {
			hasMore = true
			break
		}
	}

	return ei.index, !hasMore
}

// Exclude blank entries
func (s *Server) UserEntries() *EntriesIterator {
	return &EntriesIterator{
		s:     s,
		index: 0,
		Entry: Entry{},
	}
}

func (s *Server) AllEntries() []Entry {
	return s.log
}
