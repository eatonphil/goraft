package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/eatonphil/goraft"
	"github.com/pkg/profile"
)

type kvStateMachine struct {
	mu sync.Mutex
	kv map[string]string
}

func newKvSM() *kvStateMachine {
	return &kvStateMachine{
		kv: map[string]string{},
	}
}

func kvsmMessage_Get(key string) []byte {
	msg := make([]byte,
		3+ // Message type
			8+ // Key length
			8+ // Empty space
			len(key), // Key
	)
	copy(msg[:3], []byte("get"))
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	copy(msg[19:19+len(key)], key)
	return msg
}

func kvsmMessage_Set(key, value string) []byte {
	msg := make([]byte,
		3+ // Message type
			8+ // Key length
			8+ // Value length
			len(key)+ // Key
			len(value), // Value
	)
	copy(msg[:3], []byte("set"))
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	binary.LittleEndian.PutUint64(msg[11:19], uint64(len(value)))
	copy(msg[19:19+len(key)], key)
	copy(msg[19+len(key):19+len(key)+len(value)], value)
	return msg
}

func (kvsm *kvStateMachine) Apply(msg []byte) ([]byte, error) {
	msgType := msg[:3]
	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	key := string(msg[19 : 19+keyLen])

	var res []byte

	kvsm.mu.Lock()
	switch string(msgType) {
	case "set":
		valLen := binary.LittleEndian.Uint64(msg[11:19])
		val := string(msg[19+keyLen : 19+keyLen+valLen])
		kvsm.kv[key] = val
	case "get":
		res = []byte(kvsm.kv[key])
	}
	kvsm.mu.Unlock()

	return res, nil
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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

	sm1 := newKvSM()
	sm2 := newKvSM()
	sm3 := newKvSM()

	s1 := goraft.NewServer(cluster, sm1, ".", 0)
	s2 := goraft.NewServer(cluster, sm2, ".", 1)
	s3 := goraft.NewServer(cluster, sm3, ".", 2)

	DEBUG := false
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
	N_ENTRIES := 50_000 // 50_000 // / N_CLIENTS
	BATCH_SIZE := goraft.MAX_APPEND_ENTRIES_BATCH / N_CLIENTS
	fmt.Printf("Clients: %d. Entries: %d. Batch: %d.\n", N_CLIENTS, N_ENTRIES, BATCH_SIZE)

	var wg sync.WaitGroup
	wg.Add(N_CLIENTS)
	var randKey, randValue string
	var total time.Duration
	var mu sync.Mutex

	var allEntries [][]byte
	for j := 0; j < N_CLIENTS; j++ {
		go func(j int) {
			defer wg.Done()

			//t := time.Now()
			var entries [][]byte
			for i := 0; i < N_ENTRIES; i++ {
				// if i%1000 == 0 && i > 0 {
				// 	fmt.Printf("%d entries inserted in %s.\n", i, time.Now().Sub(t))
				// 	t = time.Now()
				// }
				key := randomString()
				value := randomString()

				if rand.Intn(100) > 90 || i == 0 && j == 0 {
					randKey = key
					randValue = value
				}

				entries = append(entries, kvsmMessage_Set(key, value))

				if len(entries) < BATCH_SIZE && i < N_ENTRIES-1 {
					continue
				}

				allEntries = append(allEntries, entries...)
			foundALeader:
				for {
					for _, s := range []*goraft.Server{s1, s2, s3} {
						t := time.Now()
						_, err := s.Apply(entries)
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
								len(entries),
								i+1,
								N_ENTRIES,
								((i+1)*100)/N_ENTRIES,
								diff,
								float64(len(entries))/(float64(diff)/float64(time.Second)),
							)
							break foundALeader
						}
					}
					time.Sleep(time.Second)
				}

				entries = [][]byte{}
			}
		}(j)
	}

	wg.Wait()
	fmt.Printf("Total time: %s. Average insert/second: %s. Throughput: %f entries/s.\n", total, total/time.Duration(N_ENTRIES), float64(N_ENTRIES)/(float64(total)/float64(time.Second)))

	for i, s := range []*goraft.Server{s1, s2, s3} {
		for !s.AllCommitted() {
			time.Sleep(time.Second)
			fmt.Println("Waiting for commits to be applied.")
		}

		fmt.Println("Validating all entries.")
		var allEntriesIndex int
		it := s.AllEntries()
		for {
			done := it.Next()
			if !bytes.Equal(it.Entry.Command, allEntries[allEntriesIndex]) {
				panic(fmt.Sprintf("Server %d. Missing or out-of-order entry at %d.\n", cluster[i].Id, allEntriesIndex))
			}
			allEntriesIndex++
			if done {
				break
			}
		}

		if allEntriesIndex != N_ENTRIES {
			panic(fmt.Sprintf("Server %d. Expected %d entries, got %d.", cluster[i].Id, N_ENTRIES, allEntriesIndex))
		}
	}

	fmt.Println("Validating state machine.")
	var v []byte
	for _, s := range []*goraft.Server{s1, s2, s3} {
		res, err := s.Apply([][]byte{kvsmMessage_Get(randKey)})
		if err == goraft.ErrApplyToLeader {
			continue
		} else if err != nil {
			panic(err)
		} else {
			v = res[0].Result
			break
		}
	}

	for _, sm := range []*kvStateMachine{sm1, sm2, sm3} {
		goraft.Assert("Each node state machine is up-to-date", string(v), sm.kv[randKey])
		goraft.Assert("Each node state machine is up-to-date", randValue, sm.kv[randKey])
	}

	fmt.Printf("%s = %s, expected: %s\n", randKey, string(v), randValue)
}
