package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eatonphil/goraft"
)

type statemachine struct {
	db *sync.Map
}

type Command struct {
	Name string
	Data map[string]string
}

func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
	// no-op
	if cmd == nil {
		return nil, nil
	}

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

	_, err = hs.raft.Apply(buf.Bytes())
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
			for _, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				clusterEntry.Id = idAddress[0]
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

	var clusterExceptCurrent []goraft.ClusterMember
	for i, cluster := range cfg.cluster {
		if i == cfg.index {
			cfg.id = cluster.Id
			cfg.address = cluster.Address
			continue
		}

		clusterExceptCurrent = append(clusterExceptCurrent, cluster)
	}
	cfg.cluster = clusterExceptCurrent

	return cfg
}

func main() {
	cfg := getConfig()

	var db sync.Map

	var sm statemachine
	sm.db = &db

	s := goraft.NewServer(cfg.id, cfg.address, time.Second*2, cfg.cluster, &sm, "metadata")
	go s.Start()

	hs := httpServer{s, &db}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	http.ListenAndServe(cfg.http, nil)
}
