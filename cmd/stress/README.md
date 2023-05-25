# Stress

The goal of this program is to automate basic correctness checks and
stress tests.

To run:

```console
$ cd cmd/stress
$ go run main.go util.go
```

With the `go-deadlock` package turned off and the default `sync`
package on, I get throughput of around 20k-40k entries/second with
this stress test.

## Background

This program runs a few servers in memory but still communicates over
localhost and actually writes/reads from disk.

The state machine tested is still a key-value state machine.

Most of the settings are configurable by editing
[./main.go](./main.go). But it's not particularly clean code.

## Tests

It attempts to do the following:

1. Start three servers and wait for a leader to be elected.
2. Insert `N_ENTRIES` in `BATCH_SIZE` across `N_CLIENTS`.
3. Validate that all servers have committed all messages they are
   aware of.
4. Validate that all servers have all entries in their log in the
   correct order that entries were inserted.
5. Shut down all servers and turn them back on. Validate that a leader has been elected.
6. Validate that all messages that were inserted before shutdown are
   committed and in the log in the correct order.
7. Shut down all servers and delete the log for one server.
8. Turn all servers back on.
9. Validate that a leader has been elected.
10. Ensure that all servers have all entries (i.e. that the deleted log has been recovered).

That is: test the basics of leader election and log replication.

One variation that I run manually at the moment is to have three
servers configured but only turn on two of them and ensure the entire
process still works (ignoring testing for entries on the down
server). This is to prove that quorum consensus works for leader
election and log replication.
