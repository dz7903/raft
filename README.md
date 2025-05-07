# COS 518 Course Project: Reproducing RPRC

## Reproducing Etcd-7331

```bash
git checkout etcd-7331
go test --run TestReadOnlyForNewLeader --tags=with_faithful_validator
```

You should see the following result:

```
raft2025/05/04 12:06:08 INFO: 1 event send message {type:MsgHeartbeat to:2 from:1 term:2 context:"ctx" }
raft2025/05/04 12:06:08 INFO: 1 event send Heartbeat to 2
raft2025/05/04 12:06:08 ERROR: 1 violation found: readIndex violation: can not attach readIndex when a leader haven't commit an entry in its own term (commitIndex=1, commitTerm=1, currentTerm=2)
panic: violation found
```

## Running benchmarks

First, clone the `etcd` main repo elsewhere:

```bash
git clone https://github.com/etcd-io/etcd
```

Second, replace the `etcd-raft` dependency with this repo. You should add the following things in `<etcd repo path>/go.mod` and `<etcd repo path>/server/go.mod`:

```
replace (
	...
	go.etcd.io/raft/v3 => <relative path to etcd-raft repo>
)
```

Third, compile with faithful validator or ellsberg (run the following command at the root of `etcd` repo):

```bash
GO_BUILD_FLAGS="-tags=with_faithful_validator -v" scripts/build.sh
# GO_BUILD_FLAGS="-tags=with_ellsberg -v" scripts/build.sh
```

Fourth, compile the benchmark tools in `etcd` (see complete instructions in <https://github.com/etcd-io/etcd/tree/main/tools/benchmark>):

```bash
go install -v ./tools/benchmark
```

Fifth, run the `etcd` local cluster:

```bash
# make sure starts from the initial state
rm -r infra*.etcd
# you may need to add `--log-level=error` and `--snapshot-count=0` into `Profile`
# to suppress debugging messages and snapshots
goreman -f Procfile start
```

Finally, run the benchmark tool:

```bash
benchmark --endpoints=localhost:2379,localhost:22379,localhost:32379 --conns=10 --clients=10 put --total=1000
```
