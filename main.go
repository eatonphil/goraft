package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	cluster []ClusterEntry
	index   int
	id      string
	address string
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

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry ClusterEntry
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

	if len(cfg.cluster) == 0 {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	var clusterExceptCurrent []ClusterEntry
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

type statemachine struct {
	db map[string]string
}

type command struct {
	name string
	data map[string]string
}

func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
	dec := gob.NewDecoder(bytes.NewReader(cmd))
	var c command
	err := dec.Decode(&c)
	if err != nil {
		return nil, err
	}

	switch c.name {
	case "set":
		s.db[c.data["key"]] = c.data["value"]
	default:
		return nil, fmt.Errorf("Unknown command: %s", c.name)
	}

	return nil, nil
}

func main() {
	cfg := getConfig()

	var sm statemachine

	s := newServer(cfg.id, cfg.address, time.Second*2, cfg.cluster, &sm, "metadata")
	s.start()
}
