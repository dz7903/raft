# COS 518 Course Project: Reproducing RPRC

## Reproducing Etcd-7331

```
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
