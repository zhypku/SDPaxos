# SDPaxos
The prototype implementation and the extended version paper of SDPaxos,
a new state machine replication protocol for efficient geo-replication.

## What's in the repo?
- The extended version of our paper. This version provides a correctness proof of our protocol (in the Appendix).
- The source code of our prototype implementation. This implementation is based on the [codebase of EPaxos](https://github.com/efficient/epaxos).

## How to build and run
```
export GOPATH=[...]/SDPaxos
go install master
go install server
go install client

bin/master &
bin/server -port 7070 &
bin/server -port 7071 &
bin/server -port 7072 &
bin/client
```
The above commands (`bin/server`) by default execute Multi-Paxos. You can add an argument `-n` to run SDPaxos. For more argument options, you can see `src/server/server.go`.

## Related paper
[SDPaxos: Building Efficient Semi-Decentralized Geo-replicated State Machines](https://dl.acm.org/citation.cfm?id=3267837) (ACM Symposium on Cloud Computing 2018, SoCC '18)