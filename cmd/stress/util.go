package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/eatonphil/goraft"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func validateAllCommitted(servers []*goraft.Server) {
	// Not a race condition, just need to wait longer than the
	// heartbeat so that the heartbeat tells all nodes about the
	// current commitindex.
	time.Sleep(time.Second)

	for _, s := range servers {
		for {
			done, completion := s.AllCommitted()
			if done {
				fmt.Printf("Server %d. All commits applied.\n", s.Id())
				break
			}
			fmt.Printf("Server %d. Waiting for commits to be applied (%f%%).\n", s.Id(), completion)
			time.Sleep(time.Second)
		}
	}
}

func validateUserEntries(servers []*goraft.Server, allEntries [][]byte, debug func([]byte) string) {
	fmt.Println("Validating all entries.")
	for _, s := range servers {
		var allEntriesIndex int = 0
		it := s.UserEntries()
		for {
			realIndex, done := it.Next()
			if s.Debug {
				fmt.Printf("Server %d. Entry: %d. %s\n", s.Id(), allEntriesIndex, debug(it.Entry.Command))
			}

			if allEntriesIndex >= len(allEntries) {
				panic(fmt.Sprintf("Server %d. Missing entry at %d (log index: %d). (Server log has %d entries.)\n", s.Id(), allEntriesIndex, realIndex, len(s.AllEntries())))
			}

			if !bytes.Equal(it.Entry.Command, allEntries[allEntriesIndex]) {
				fmt.Println("Got:", debug(it.Entry.Command), "Wanted:", debug(allEntries[allEntriesIndex]))
				panic(fmt.Sprintf("Server %d. Missing or out-of-order entry at %d (log index: %d). (Server log has %d entries.)\n", s.Id(), allEntriesIndex, realIndex, len(s.AllEntries())))
			}

			allEntriesIndex++
			if done {
				break
			}
		}

		if allEntriesIndex != len(allEntries) {
			panic(fmt.Sprintf("Server %d. Expected %d entries, got %d.", s.Id(), len(allEntries), allEntriesIndex))
		}
	}
}

func waitForLeader(servers []*goraft.Server) uint64 {
	for {
		for _, s := range servers {
			if s.IsLeader() {
				return s.Id()
			}
		}

		fmt.Println("Waiting for a leader.")
		time.Sleep(time.Second)
	}

	return uint64(0)
}
