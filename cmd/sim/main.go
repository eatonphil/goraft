package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/eatonphil/goraft"
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

	N_ENTRIES := 3_000

	var randKey, randValue string
	t := time.Now()
	total := time.Now()
	for i := 0; i < N_ENTRIES; i++ {
		if i%1000 == 0 && i > 0 {
			fmt.Printf("%d entries inserted in %s.\n", i, time.Now().Sub(t))
			t = time.Now()
		}
		key := randomString()
		value := randomString()

		if rand.Intn(100) > 90 || i == 0 {
			randKey = key
			randValue = value
		}

	foundALeader:
		for {
			for _, s := range []*goraft.Server{s1, s2, s3} {
				_, err := s.Apply(kvsmMessage_Set(key, value))
				if err == goraft.ErrApplyToLeader {
					continue
				} else if err != nil {
					panic(err)
				} else {
					break foundALeader
				}
			}
			time.Sleep(time.Second)
		}

		goraft.Assert("Quorum reached", s1.Entries() == s2.Entries() || s1.Entries() == s3.Entries() || s2.Entries() == s3.Entries(), true)
	}
	fmt.Printf("Total time: %s. Average insert/second: %s.\n", time.Now().Sub(total), time.Now().Sub(total)/time.Duration(N_ENTRIES))

	var v []byte
	var err error
	for _, s := range []*goraft.Server{s1, s2, s3} {
		v, err = s.Apply(kvsmMessage_Get(randKey))
		if err == goraft.ErrApplyToLeader {
			continue
		} else if err != nil {
			panic(err)
		} else {
			break
		}
	}

	// Ooph, but need to wait for the other state machines to catch up.
	time.Sleep(2 * time.Second)

	for _, sm := range []*kvStateMachine{sm1, sm2, sm3} {
		goraft.Assert("Each node state machine is up-to-date", string(v), sm.kv[randKey])
		goraft.Assert("Each node state machine is up-to-date", randValue, sm.kv[randKey])
	}

	goraft.Assert("Quorum reached", s1.Entries() == s2.Entries() || s1.Entries() == s3.Entries() || s2.Entries() == s3.Entries(), true)

	fmt.Printf("%s = %s, expected: %s\n", randKey, string(v), randValue)
}
