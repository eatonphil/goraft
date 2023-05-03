# goraft

A minimal implementation of Raft in Go. [Demo video](https://www.youtube.com/watch?v=cIR8RoN2LDU).

NOT FOR PRODUCTION USE.

Things that are missing:

* Snapshotting/checkpointing and associated state transfer
* Configuration change protocol
* Rigged up to Jepsen
* Surely much else

## Simulator

Not particularly aggressive yet but does some minimal testing.

```console
$ cd cmd/sim
$ go run main.go
```

# Distributed Key-Value Store API

Try out the builtin distributed key-value API.

```console
$ cd cmd/kvapi && go build
$ rm *.dat
```

## Terminal 1

```console
$ ./kvapi --node 1 --http :2021 --cluster "0,:3030;1,:3031;2,:3032"
```

## Terminal 2

```console
$ ./kvapi --node 1 --http :2021 --cluster "0,:3030;1,:3031;2,:3032"
```

## Terminal 3

```console
$ ./kvapi --node 2 --http :2021 --cluster "0,:3030;1,:3031;2,:3032"
```

## Terminal 4

To set a key:

```console
$ curl -v http://localhost:2020/set -d '{"key": "y", "value": "hello"}' -X POST
```

To get a key:

```console
$ curl -v http://localhost:2021/get\?key\=y
```

# References

* [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf): The Raft paper.
* [raft.tla](https://github.com/ongardie/raft.tla/blob/master/raft.tla): Diego Ongaro's TLA+ spec for Raft.
* Jon Gjengset's [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
* Jack Vanlightly's [Detecting Bugs in Data Infrastructure using Formal Methods (TLA+ Series Part 1)](https://medium.com/splunk-maas/detecting-bugs-in-data-infrastructure-using-formal-methods-704fde527c58): An intro to TLA+.

Other useful implementations to peer at:

* Hashicorp's [Raft implementation](https://github.com/hashicorp/raft) in Go: Although it's often quite complicated to learn from since it actually is intended for production.
* Eli Bendersky's [Raft implementation](https://github.com/eliben/raft) in Go: Although it gets confusing because it uses negative numbers for terms whereas the paper does not.
* Jing Yang's [Raft implementation](https://github.com/ditsing/ruaft) in Rust.