package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/eatonphil/goraft"
	"github.com/pkg/profile"
)

type arrayStateMachine struct {
	mu sync.Mutex
	a  []string
}

func (asm *arrayStateMachine) Apply(msg []byte) ([]byte, error) {
	asm.mu.Lock()
	defer asm.mu.Unlock()

	asm.a = append(asm.a, string(msg))
	return nil, nil
}

func newArrayStateMachine() *arrayStateMachine {
	return &arrayStateMachine{
		mu: sync.Mutex{},
		a:  make([]string, 0, 1_000_000),
	}
}

func main() {
	defer profile.Start(profile.MemProfile).Stop()
	rand.Seed(0)

	cluster := []goraft.ClusterMember{
		{
			Id:      1,
			Address: "localhost:2020",
		},
		{
			Id:      2,
			Address: "localhost:2021",
		},
		{
			Id:      3,
			Address: "localhost:2022",
		},
	}

	sm1 := newArrayStateMachine()
	sm2 := newArrayStateMachine()
	sm3 := newArrayStateMachine()

	s1 := goraft.NewServer(cluster, sm1, ".", 0)
	s2 := goraft.NewServer(cluster, sm2, ".", 1)
	s3 := goraft.NewServer(cluster, sm3, ".", 2)

	DEBUG := true
	s1.Debug = DEBUG
	s1.Start()
	s2.Debug = DEBUG
	s2.Start()
	s3.Debug = DEBUG
	s3.Start()

outer:
	for {
		for _, s := range []*goraft.Server{s1, s2, s3} {
			if s.IsLeader() {
				break outer
			}
		}

		fmt.Println("Waiting for a leader.")
		time.Sleep(time.Second)
	}

	N_CLIENTS := 1
	N_ENTRIES := 20 // 50_000 // / N_CLIENTS
	BATCH_SIZE := 1 // goraft.MAX_APPEND_ENTRIES_BATCH / N_CLIENTS
	fmt.Printf("Clients: %d. Entries: %d. Batch: %d.\n", N_CLIENTS, N_ENTRIES, BATCH_SIZE)

	var wg sync.WaitGroup
	wg.Add(N_CLIENTS)
	var total time.Duration
	var mu sync.Mutex

	var allEntries [][]byte
	for i := 0; i < N_ENTRIES; i++ {
		allEntries = append(allEntries, []byte(randomString()))
	}

	servers := []*goraft.Server{s1, s2, s3}

	debugEntry := func(msg []byte) string {
		return string(msg)
	}

	for j := 0; j < N_CLIENTS; j++ {
		go func(j int) {
			defer wg.Done()

			for i := 0; i < N_ENTRIES; i += BATCH_SIZE {
				end := i + BATCH_SIZE
				if end > len(allEntries) {
					end = len(allEntries)
				}
				batch := allEntries[i:end]
			foundALeader:
				for {
					for _, s := range []*goraft.Server{s1, s2, s3} {
						t := time.Now()
						_, err := s.Apply(batch)
						if err == goraft.ErrApplyToLeader {
							continue
						} else if err != nil {
							panic(err)
						} else {
							diff := time.Now().Sub(t)
							mu.Lock()
							total += diff
							mu.Unlock()
							fmt.Printf("Client: %d. %d entries (%d of %d: %d%%) inserted. Latency: %s. Throughput: %f entries/s.\n",
								j,
								len(batch),
								i+1,
								N_ENTRIES,
								((i+1)*100)/N_ENTRIES,
								diff,
								float64(len(batch))/(float64(diff)/float64(time.Second)),
							)

							validateAllCommitted(servers)
							validateAllEntries(servers, allEntries[:end], debugEntry)
							break foundALeader
						}
					}
					time.Sleep(time.Second)
				}
			}
		}(j)
	}

	wg.Wait()
	fmt.Printf("Total time: %s. Average insert/second: %s. Throughput: %f entries/s.\n", total, total/time.Duration(N_ENTRIES), float64(N_ENTRIES)/(float64(total)/float64(time.Second)))

	validateAllCommitted(servers)

	for j, entry := range allEntries {
		for i, sm := range []*arrayStateMachine{sm1, sm2, sm3} {
			goraft.Assert(fmt.Sprintf("Server %d state machine is up-to-date on entry %d.", cluster[i].Id, j), string(entry), sm.a[j])
		}
	}
}
