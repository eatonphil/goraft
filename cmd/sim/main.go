package main

import "encoding/binary"
import "fmt"
import "sync"
import "math/rand"

import "github.com/eatonphil/goraft"

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

func main() {
	rand.Seed(0)

	cluster := []goraft.ClusterMember{
		{
			Id:      "0",
			Address: ":2020",
		},
		{
			Id:      "1",
			Address: ":2021",
		},
		{
			Id:      "2",
			Address: ":2022",
		},
	}

	statemachine := newKvSM()

	s1 := goraft.NewServer(cluster, statemachine, ".", 0)
	s1.MakeLeader()
	s2 := goraft.NewServer(cluster, statemachine, ".", 1)
	s3 := goraft.NewServer(cluster, statemachine, ".", 2)

	s1.Start()
	s2.Start()
	s3.Start()

	_, err := s1.Apply(kvsmMessage_Set("a", "1"))
	if err != nil {
		panic(err)
	}

	_, err = s1.Apply(kvsmMessage_Set("b", "2"))
	if err != nil {
		panic(err)
	}

	v, err := s1.Apply(kvsmMessage_Get("a"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("a = %s\n", string(v))
}
