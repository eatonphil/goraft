package main

import "encoding/binary"
import "bytes"
import "sync"
import "time"
import "math/rand"

import "github.com/eatonphil/goraft"

type kvStateMachine struct {
	mu sync.Mutex
	kv map[string]string
}

func kvsmMessage_Get(key) []byte {
	msg := make([]byte(
		3              // Message type
		+ 8            // Key length
		+ 8            // Empty space
		+ len(key)     // Key
	)
	copy(msg[:3], []byte("get"))
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	copy(msg[19:19+keyLen], key)
}

func kvsmMessage_Set(key, value []byte) []byte {
	msg := make([]byte(
		3              // Message type
		+ 8            // Key length
		+ 8            // Value length
		+ len(key)     // Key
		+ len(value)), // Value
	)
	copy(msg[:3], []byte("set"))
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	binary.LittleEndian.PutUint64(msg[11:19], uint64(len(value)))
	copy(msg[19:19+keyLen], key)
	copy(msg[19+keyLen:19+keyLen+valLen], value)
}

func (kvsm kvStateMachine) Apply(msg []byte) ([]byte, error) {
	msgType := msg[:3]
	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	key := msg[19:19+keyLen]

	var res []byte

	kvsm.mu.Lock()
	switch string(msg[:3]) {
	case "set":
		valLen := binary.LittleEndian.Uint64(msg[11:19])
		val := msg[19+keyLen:19+keyLen+valLen]
		kvsm.kv[key] = val
	case "get":
		res = kvsm.kv[key]
	}
	kvsm.mu.Unlock()

	return res, nil
}

func main() {
	rand.Seed(0)

	cluster := []ClusterMember{
		{
			"0",
			":2020",
		},
		{
			"1",
			":2021",
		},
		{
			"2",
			":2022",
		},
	}
	
	s1 := NewServer(cluster, statemachine, ".", 0)
	s1.MakeLeader()
	s2 := NewServer(cluster, statemachine, ".", 1)
	s3 := NewServer(cluster, statemachine, ".", 2)

	s1.Start()
	s2.Start()
	s3.Start()
	
	_, err := s1.Apply(kvsmMessage_Set([]byte("a"), []byte("1")))
	if err != nil {
		panic(err)
	}

	_, err = s1.Apply(kvsmMessage_Set([]byte("b"), []byte("2")))
	if err != nil {
		panic(err)
	}

	v, err := s1.Apply(kvsmMessage_Set([]byte("a")))
	if err != nil {
		panic(err)
	}

	fmt.Printf("a = %s", string(v))
}
