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

func encodeKvsmMessage_Get(key string) []byte {
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

func decodeKvsmMessage_Get(msg []byte) (bool, string) {
	msgType := msg[:3]
	if !bytes.Equal(msgType, []byte("get")) {
		return false, ""
	}

	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	key := string(msg[19 : 19+keyLen])

	return true, key
}

func encodeKvsmMessage_Set(key, value string) []byte {
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

func decodeKvsmMessage_Set(msg []byte) (bool, string, string) {
	msgType := msg[:3]
	if !bytes.Equal(msgType, []byte("set")) {
		return false, "", ""
	}

	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	key := string(msg[19 : 19+keyLen])

	valLen := binary.LittleEndian.Uint64(msg[11:19])
	val := string(msg[19+keyLen : 19+keyLen+valLen])

	return true, key, val
}

func (kvsm *kvStateMachine) Apply(msg []byte) ([]byte, error) {
	kvsm.mu.Lock()
	defer kvsm.mu.Unlock()

	if ok, key, val := decodeKvsmMessage_Set(msg); ok {
		kvsm.kv[key] = val
		return nil, nil
	} else if ok, key := decodeKvsmMessage_Get(msg); ok {
		return []byte(kvsm.kv[key]), nil
	} else {
		return nil, fmt.Errorf("Unknown state machine message: %x", msg)
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

	sm1 := newKvSM()
	sm2 := newKvSM()
	sm3 := newKvSM()

	s1 := goraft.NewServer(cluster, sm1, ".", 0)
	//s1.RPCProxy = goraft.NewSlowRPCProxy(s1)
	s2 := goraft.NewServer(cluster, sm2, ".", 1)
	//s2.RPCProxy = goraft.NewSlowRPCProxy(s2)
	s3 := goraft.NewServer(cluster, sm3, ".", 2)
	//s3.RPCProxy = goraft.NewSlowRPCProxy(s3)

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
	var total time.Duration
	var mu sync.Mutex

	var allEntries [][]byte
	for i := 0; i < N_ENTRIES; i++ {
		key := randomString()
		value := randomString()

		allEntries = append(allEntries, encodeKvsmMessage_Set(key, value))
	}

	servers := []*goraft.Server{s1, s2, s3}
	sms := []*kvStateMachine{sm1, sm2, sm3}

	// allEntries := [][]byte{
	// 	encodeKvsmMessage_Set("a", "1"),
	// 	encodeKvsmMessage_Set("b", "2"),
	// 	encodeKvsmMessage_Set("c", "3"),
	// }

	debugEntry := func(entry []byte) string {
		if ok, key, val := decodeKvsmMessage_Set(entry); ok {
			return fmt.Sprintf("Key: %s. Value: %s.", key, val)
		} else if ok, key := decodeKvsmMessage_Get(entry); ok {
			return fmt.Sprintf("Key: %s.", key)
		}

		return "Unknown."
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
	validateAllEntries(servers, allEntries, debugEntry)

	for j, entry := range allEntries {
		_, key, value := decodeKvsmMessage_Set(entry)
		for i := range servers {
			sm := sms[i]
			goraft.Assert(fmt.Sprintf("Server %d state machine is up-to-date on entry %d (%s).", cluster[i].Id, j, key), value, sm.kv[key])
		}
	}

	fmt.Println("Validating get.")

	var v []byte
	var key, value string
	for _, s := range servers {
		_, key, value = decodeKvsmMessage_Set(allEntries[0])
		res, err := s.Apply([][]byte{encodeKvsmMessage_Get(key)})
		if err == goraft.ErrApplyToLeader {
			continue
		} else if err != nil {
			panic(err)
		} else {
			v = res[0].Result
			break
		}
	}

	fmt.Printf("%s = %s, expected: %s\n", key, string(v), value)
}
