package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/eatonphil/goraft"
)

type statemachine struct {
	db     *sync.Map
	server int
}

type Command struct {
	Name string
	Data map[string]string
}

func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(cmd))
	var c Command
	err := dec.Decode(&c)
	if err != nil {
		return nil, err
	}

	switch c.Name {
	case "set":
		s.db.Store(c.Data["key"], c.Data["value"])
	default:
		return nil, fmt.Errorf("Unknown Command: %s", c.Name)
	}

	return nil, nil
}

type httpServer struct {
	raft *goraft.Server
	db   *sync.Map
}

// Example:
//
//	curl http://localhost:2020/set -d '{"key": "x", "value": "1"}' -X POST
func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var c Command
	c.Name = "set"
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&c.Data)
	if err != nil {
		log.Printf("Could not read key-value in http request: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err = enc.Encode(c)
	if err != nil {
		log.Printf("Could not encode raft Command: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	_, err = hs.raft.Apply([][]byte{buf.Bytes()})
	if err != nil {
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, _ := hs.db.Load(key)
	if value == nil {
		value = ""
	}

	rsp := struct {
		Data string `json:"data"`
	}{value.(string)}
	err := json.NewEncoder(w).Encode(rsp)
	if err != nil {
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

type config struct {
	cluster []goraft.ClusterMember
	index   int
	id      string
	address string
	http    string
}

func getConfig() config {
	cfg := config{}
	var node string
	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatal("Expected $value to be a valid integer in `--node $value`, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--http" {
			cfg.http = os.Args[i+2]
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry goraft.ClusterMember
			for i, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				clusterEntry.Id = uint64(i)
				clusterEntry.Address = idAddress[1]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}

			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("Missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("Missing required parameter: --http $address")
	}

	if len(cfg.cluster) == 0 {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	return cfg
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	cfg := getConfig()

	var db sync.Map

	var sm statemachine
	sm.db = &db
	sm.server = cfg.index

	s := goraft.NewServer(cfg.cluster, &sm, ".", cfg.index)
	s.Debug = true
	go s.Start()

	hs := httpServer{s, &db}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	err = http.ListenAndServe(cfg.http, nil)
	if err != nil {
		panic(err)
	}
}
