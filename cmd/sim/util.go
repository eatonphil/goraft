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
	fmt.Printf("Validating all committed for %d servers.\n", len(servers))

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

func validateAllEntries(servers []*goraft.Server, allEntries [][]byte, debug func([]byte) string) {
	fmt.Println("Validating all entries.")
	for _, s := range servers {
		retries := 3
	retry:
		for retries > 0 {
			if retries < 3 {
				time.Sleep(30 * time.Second)
				fmt.Println("Waiting and retrying.")
			}
			retries--

			var allEntriesIndex int
			it := s.AllEntries()
			for {
				done := it.Next()
				if s.Debug {
					fmt.Printf("Server %d. Entry: %d. %s\n", s.Id(), allEntriesIndex, debug(it.Entry.Command))
				}

				if allEntriesIndex >= len(allEntries) {
					fmt.Printf("Server %d. More entries stored than were sent in: %d.\n", s.Id(), allEntriesIndex)
					continue retry
				}

				if !bytes.Equal(it.Entry.Command, allEntries[allEntriesIndex]) {
					fmt.Printf(
						"Server %d. Missing or out-of-order entry at %d (got: '%s', wanted: %s).\n",
						s.Id(),
						allEntriesIndex,
						string(it.Entry.Command),
						string(allEntries[allEntriesIndex]),
					)
					continue retry
				}

				allEntriesIndex++
				if done {
					break
				}
			}

			if allEntriesIndex != len(allEntries) {
				fmt.Printf("Server %d. Expected %d entries, got %d.\n", s.Id(), len(allEntries), allEntriesIndex)
				continue retry
			}

			break retry
		}

		if retries == 0 {
			panic("Failed with too many retries.")
		}
	}
}
