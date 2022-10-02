# goraft (WIP)

A minimal implementation of Raft in Go.

NOT FOR PRODUCTION USE.

Try out the builtin distributed key-value API.

```bash
$ cd cmd/kvapi && go build
$ rm -rf metadata && mkdir metadata
```

In terminal 1:
```
./cmd/kvapi/kvapi --http :9000 --node 0 --cluster "0,:8080;1,:8081;2,:8082"
```

In terminal 2:

```bash
./cmd/kvapi/kvapi --http :9001 --node 1 --cluster "0,:8080;1,:8081;2,:8082"
```

In terminal 3:

```bash
./cmd/kvapi/kvapi --http :9002 --node 2 --cluster "0,:8080;1,:8081;2,:8082"
```

Then in terminal 4:
