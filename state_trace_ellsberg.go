//go:build with_ellsberg

package raft

import (
	"encoding/gob"
	"hash/fnv"
	"sort"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"reflect"
)

const (
	Leader raftRole = iota
	Candidate
	Follower
)

type raftRole int

type set[T comparable] map[T]struct{}

func (s set[T]) add(e T) {
	s[e] = struct{}{}
}

func (s set[T]) delete(e T) {
	delete(s, e)
}

func (s set[T]) copy() set[T] {
	t := make(set[T])
	for e := range s {
		t.add(e)
	}
	return t
}

func (s set[T]) equal(t set[T]) bool {
	return reflect.DeepEqual(s, t)
}

type queue[T any] []T

func (q queue[T]) enqueue(e T) {
	q = append(q, e)
}

func (q queue[T]) dequeue() T {
	e := q[0]
	// TODO: resize q when cap(q) > 2 * len(q)
	q = q[1:]
	return e
}

func (q queue[T]) empty() bool {
	return len(q) == 0
}

type raftState struct {
	id             uint64
	role           raftRole
	nodes          map[uint64]struct{}
	term           uint64
	vote           uint64
	commit         uint64
	config         *tracker.Config
	votesResponded map[uint64]struct{}
	votesGranted   map[uint64]struct{}
	matchIndex     map[uint64]uint64
	log            []pb.Entry
	commitIndex    uint64
	latestMsg      *pb.Message
	disabled       bool
	logger         Logger
}

func raftInit(r *raft) raftState {
	s := raftState{
		id:             r.id,
		nodes:          r.trk.Voters.IDs(),
		term:           r.Term,
		role:           Follower,
		vote:           r.Vote,
		votesResponded: make(map[uint64]struct{}),
		votesGranted:   make(map[uint64]struct{}),
		log:            r.raftLog.allEntries(),
		disabled:       false,
		logger:         r.logger,
	}
	return s
}

func equalNodeSet(a, b map[uint64]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func equalNestedIntMap(a, b map[int]map[int]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || len(va) != len(vb) {
			return false
		}
		for kk := range va {
			if _, exists := vb[kk]; !exists {
				return false
			}
		}
	}
	return true
}

func equalUint64Map(a, b map[uint64]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func equalLogEntries(a, b []pb.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if &a[i] == &b[i] {
			return false
		}
	}
	return true
}

func (s *raftState) equal(t *raftState) bool {

	return true
}

func writeSortedUint64Set(enc *gob.Encoder, m map[uint64]struct{}) {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	_ = enc.Encode(keys)
}

func writeSortedUint64Map(enc *gob.Encoder, m map[uint64]uint64) {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		_ = enc.Encode(k)
		_ = enc.Encode(m[k])
	}
}

func (s *raftState) hash() uint64 {
	h := fnv.New64a()
	enc := gob.NewEncoder(h)

	// Hash simple fields
	_ = enc.Encode(s.id)
	_ = enc.Encode(s.role)
	_ = enc.Encode(s.term)
	_ = enc.Encode(s.vote)
	_ = enc.Encode(s.commit)
	_ = enc.Encode(s.commitIndex)
	_ = enc.Encode(s.disabled)

	// Hash nodes (map[uint64]struct{}) in a deterministic order
	writeSortedUint64Set(enc, s.nodes)
	writeSortedUint64Set(enc, s.votesGranted)
	writeSortedUint64Set(enc, s.votesResponded)

	// Hash matchIndex deterministically
	writeSortedUint64Map(enc, s.matchIndex)

	// Hash config if not nil
	if s.config != nil {
		_ = enc.Encode(s.config)
	}

	// Hash log
	_ = enc.Encode(s.log)

	// Hash latestMsg if not nil
	if s.latestMsg != nil {
		_ = enc.Encode(s.latestMsg)
	}

	// Logger is intentionally skipped

	return h.Sum64()
}

func (v *raftState) receiveAppendEntriesResp(m *pb.Message, _ bool) {
	from := m.From
	// TLA+ specification does not assert node to be Leader
	if !m.Reject {
		v.matchIndex[from] = max(v.matchIndex[from], m.Index)
	}
}

func (s *raftState) apply(m *pb.Message) *raftState {
	s.logger.Infof("%d event receive message {%+v}", s.id, m)
	if m.To != s.id {
		return s
	}
	from := m.From
	if m.Term > s.term {
		s.logger.Info("%d event become follower", s.id)
		s.role = Follower
		s.term = m.Term
		// continue to process this message
	}
	switch m.Type {
	case pb.MsgVote:
		s.logger.Infof("%d event receive RequestVote from %d", s.id, from)
		//s.assertf(s.latestMsg == nil, "message {%+v} not processed immediately", s.latestMsg)
		s.latestMsg = m
	case pb.MsgVoteResp:
		s.logger.Infof("%d event receive RequestVoteResp from %d", s.id, from)
		if m.Term < s.term {
			// DropStaleResponse
			s.logger.Infof("%d event drop stale response {%+v}", s.id, m)
		} else {
			s.votesResponded[from] = struct{}{}
			if !m.Reject {
				s.votesGranted[from] = struct{}{}
			}
		}
	case pb.MsgHeartbeat:
		s.logger.Infof("%d event receive Heartbeat from %d", s.id, from)
		if m.Term == s.term && s.role == Candidate {
			s.logger.Infof("%d event ReturnToFollowerState", s.id)
			s.role = Follower
		} else {
			s.latestMsg = m
		}
	case pb.MsgHeartbeatResp:
		s.logger.Infof("%d event receive HeartbeatResp from %d", s.id, from)
		if m.Term < s.term {
			// DropStaleResponse
			s.logger.Infof("%d event drop stale response {%+v}", s.id, m)
		} else {
			s.receiveAppendEntriesResp(m, true)
		}
	case pb.MsgApp:
		s.logger.Infof("%d event receive AppendEntries from %d", s.id, from)
		if m.Term == s.term && s.role == Candidate {
			s.logger.Infof("%d event ReturnToFollowerState", s.id)
			s.role = Follower
		} else {
			s.latestMsg = m
		}
	case pb.MsgAppResp:
		s.logger.Infof("%d event receive AppendEntriesResp from %d", s.id, from)
		if m.Term < s.term {
			// DropStaleResponse
			s.logger.Infof("%d event drop stale response {%+v}", s.id, m)
		} else {
			s.receiveAppendEntriesResp(m, false)
		}
	}
	return s
}

func (s *raftState) timeout() *raftState {
	s.logger.Infof("%d event timeout", s.id)
	s.role = Candidate
	s.term = s.term + 1
	s.logger.Infof("%d current term is %d", s.id, s.term)
	s.vote = s.id
	s.votesResponded = make(map[uint64]struct{})
	s.votesGranted = make(map[uint64]struct{})
	return s
}

func (s *raftState) canApplyAsap(m *pb.Message) bool {
	return false
}

type partialRaftState struct {
	id             *uint64
	role           *raftRole
	nodes          map[uint64]struct{}
	term           *uint64
	vote           *uint64
	commit         *uint64
	config         *tracker.Config
	votesResponded map[uint64]struct{}
	votesGranted   map[uint64]struct{}
	matchIndex     map[uint64]uint64
	log            []pb.Entry
	commitIndex    *uint64
	latestMsg      *pb.Message
	disabled       *bool
	logger         Logger
}

func (s *partialRaftState) contains(t *raftState) bool {
	if s == nil || t == nil {
		return false
	}

	if s.id != nil && *s.id != t.id {
		return false
	}
	if s.role != nil && *s.role != t.role {
		return false
	}
	if s.term != nil && *s.term != t.term {
		return false
	}
	if s.vote != nil && *s.vote != t.vote {
		return false
	}
	if s.commit != nil && *s.commit != t.commit {
		return false
	}
	if s.commitIndex != nil && *s.commitIndex != t.commitIndex {
		return false
	}
	if s.disabled != nil && *s.disabled != t.disabled {
		return false
	}
	/*if s.config != nil && (t.config == nil || !s.config.Equal(t.config)) {
		return false
	}
	if s.latestMsg != nil && (t.latestMsg == nil || !proto.Equal(s.latestMsg, t.latestMsg)) {
		return false
	}*/
	if s.nodes != nil && !equalNodeSet(s.nodes, t.nodes) {
		return false
	}
	if s.votesResponded != nil && !equalNodeSet(s.votesResponded, t.votesResponded) {
		return false
	}
	if s.votesGranted != nil && !equalNodeSet(s.votesGranted, t.votesGranted) {
		return false
	}
	if s.matchIndex != nil && !equalUint64Map(s.matchIndex, t.matchIndex) {
		return false
	}
	/*if s.log != nil && !s.log.Equals(t.log) {
		return false
	}*/
	// logger is typically excluded from equality checks
	return true
}

func inferInducing(m *pb.Message) partialRaftState {
	/*to := m.To
	switch m.Type {
	case pb.MsgVote:
		s := partialRaftState{}
		s.role = &1
		return s
		//v.assertf(v.id != to, "can't send RequestVote to self")
	case pb.MsgVoteResp:
		s := partialRaftState{}
		s.role = &Candidate
		s.term = &m.term
		//ret = append(ret, s)
		//s2 := partialRaftState{}
		//s2.term = &m.term
		//ret = append(ret, s2)
		/*if to == v.id {
			v.assertf(v.role == Candidate, "only Candidate can send RequestVoteResp to itself")
			v.assert(v.term == m.Term)
			v.assert(!event.message.Reject)
		} else {
			v.assertf(v.latestMsg != nil && v.latestMsg.Type == pb.MsgVote, "expect RequestVote received")
			m1 := v.latestMsg
			logOk := m1.LogTerm > v.lastTerm() || m1.LogTerm == v.lastTerm() && m1.Index >= uint64(len(v.log))
			grant := logOk && m1.Term == v.term && (v.vote == None || v.vote == m1.From)
			v.assertf(m.Term == v.term, "resp message term %d, current term %d", m.Term, v.term)
			v.assert(m.Reject == !grant)
			v.latestMsg = nil
		}
		return s
	case pb.MsgHeartbeat:
		s := partialRaftState{}
		s.role == &Leader
		s.term = &m.term
		s.log = make(r.raftlog)
		if m.Index > 0 && m.Index <= uint64(len(v.log)) {
			s.log[m.Index-1].Term = m.Logterm
		}
		for i, e := range m.Entries {
			// we do not check the content of entries, only indexes and terms
			s.log[m.Index+uint64(i)].Index = e.Index
			s.log[m.Index+uint64(i)].Term = e.Term
		}
		s.commitIndex = &m.Commit
		return s
	case pb.MsgHeartbeatResp:
		//v.sendAppendEntriesResp(m, true)
	case pb.MsgApp:
		s := partialRaftState{}
		s.role == &Leader
		s.term = &m.term
		s.log = make(r.raftlog)
		if m.Index > 0 && m.Index <= uint64(len(v.log)) {
			s.log[m.Index-1].Term = m.Logterm
		}
		for i, e := range m.Entries {
			// we do not check the content of entries, only indexes and terms
			s.log[m.Index+uint64(i)].Index = e.Index
			s.log[m.Index+uint64(i)].Term = e.Term
		}
		s.commitIndex = &m.Commit
		return s
	case pb.MsgAppResp:
		v.sendAppendEntriesResp(m, true)
		if s.id == m.To {
			s.role = Leader
			s.term = m.Term
			m.Index = v.log
			v.assert(m.Index == uint64(len(v.log)))
		} else {
			v.sendAppendEntriesResp(m, false)
		}
	} */
	s := partialRaftState{}
	return s
}

func maybeReachable(s *raftState, pending set[*pb.Message], m *pb.Message, t *partialRaftState) bool {
	if t.term != nil && s.term > *t.term {
		return false
	}
	return true
}

func maybeReachableTimeout(s *raftState, pending set[*pb.Message], t *partialRaftState) bool {
	if t.term != nil && s.term > *t.term {
		return false
	}
	return true
}

type simState struct {
	state raftState
	// remember, timeout is always pending but not recorded here
	// TODO: make it set of sets
	pendings set[*pb.Message]
}

func (s *simState) pruneAsap() {
	for m := range s.pendings {
		if s.state.canApplyAsap(m) {
			s.state = *s.state.apply(m)
			s.pendings.delete(m)
		}
	}
}

func (s *simState) addPending(m *pb.Message) {
	s.pendings.add(m)
}

// TODO: lookahead
func (s *simState) findReachable(t *partialRaftState, newInstances set[*simState]) {

	q := make(queue[*simState], 0)
	explored := make(map[uint64][]*simState)
	q = append(q, s)
	for !q.empty() {
		ss := q[0]
		q = q[1:]
		ss.state.logger.Infof("%d ellsberg: violation detected; no state in %+v can produce msg %+v", ss.state.id, ss.state.term, ss.state.vote)
		explored[ss.state.hash()] = append(explored[ss.state.hash()], ss)
		if t.contains(&ss.state) {
			newInstances.add(ss)
			continue
		}
		for m := range ss.pendings {
			if maybeReachable(&ss.state, ss.pendings, m, t) {
				newss := simState{state: *ss.state.apply(m), pendings: ss.pendings.copy()}
				newss.pendings.delete(m)
				newss.pruneAsap()
				if _, ok := explored[newss.state.hash()]; !ok {
					hasExplored := false
					for _, ss1 := range explored[newss.state.hash()] {
						if ss1.state.equal(&newss.state) && ss1.pendings.equal(newss.pendings) {
							hasExplored = true
						}
					}
					if !hasExplored {
						q.enqueue(&newss)
						explored[newss.state.hash()] = append(explored[newss.state.hash()], &newss)
					}
				}
			}
		}
		if maybeReachableTimeout(&ss.state, ss.pendings, t) {
			newss := simState{state: *ss.state.timeout(), pendings: ss.pendings.copy()}
			newss.pruneAsap()
			if _, ok := explored[newss.state.hash()]; !ok {
				hasExplored := false
				for _, ss1 := range explored[newss.state.hash()] {
					if ss1.state.equal(&newss.state) && ss1.pendings.equal(newss.pendings) {
						hasExplored = true
					}
				}
				if !hasExplored {
					q.enqueue(&newss)
					explored[newss.state.hash()] = append(explored[newss.state.hash()], &newss)
				}
			}
		}
	}
}

type EllsbergState struct {
	id           uint64
	instances    set[*simState]
	incomingMsgC chan *pb.Message
	outgoingMsgC chan *pb.Message
	logger       Logger
}

func ellsbergInit(r *raft) EllsbergState {
	// TODO: should we use buffered channels?
	incomingMsgC := make(chan *pb.Message)
	outgoingMsgC := make(chan *pb.Message)
	instances := make(set[*simState])
	instances.add(&simState{state: raftInit(r), pendings: make(set[*pb.Message])})
	return EllsbergState{
		id:           r.id,
		instances:    instances,
		incomingMsgC: incomingMsgC,
		outgoingMsgC: outgoingMsgC,
		logger:       r.logger,
	}
}

func (s *EllsbergState) mainLoop() {
	for {
		var m *pb.Message
		select {
		case m = <-s.incomingMsgC:
			s.processIncoming(m)
		case m = <-s.outgoingMsgC:
			s.processOutgoing(m)
		}
	}
}

func (s *EllsbergState) processIncoming(m *pb.Message) {
	for ss := range s.instances {
		if ss.state.canApplyAsap(m) {
			ss.state = *ss.state.apply(m)
			ss.pruneAsap()
		} else {
			ss.addPending(m)
		}
	}
}

func (s *EllsbergState) processOutgoing(m *pb.Message) {
	t := inferInducing(m)
	newInstances := make(set[*simState])
	for ss := range s.instances {
		ss.findReachable(&t, newInstances)
	}
	if len(newInstances) == 0 {
		s.logger.Fatalf("%d ellsberg: violation detected; no state in %+v can produce msg %+v", s.id, s.instances, m)
	}
	s.instances = newInstances
}

type MyTraceLogger struct {
	ellsbergState EllsbergState
}

func (l *MyTraceLogger) getEllsbergState() *EllsbergState {
	return &l.ellsbergState
}

type TraceLogger interface {
	getEllsbergState() *EllsbergState
}

func traceInitState(r *raft) {
	if r.traceLogger == nil {
		s := ellsbergInit(r)
		r.traceLogger = &MyTraceLogger{ellsbergState: s}
	}
	go r.traceLogger.getEllsbergState().mainLoop()
}

func traceRecoverState(r *raft) {
	r.logger.Infof("%d event recover state", r.id)
	s := r.traceLogger.getEllsbergState()
	for ss := range s.instances {
		ss.state.role = Follower
		ss.state.term = r.Term
		ss.state.vote = r.Vote
		ss.state.commitIndex = r.raftLog.committed
	}
}

func traceReady(*raft) {}

func traceCommit(r *raft) {
}

func traceReplicate(r *raft, e ...pb.Entry) {
}

func traceBecomeFollower(r *raft) {}

func traceBecomeCandidate(r *raft) {
}

func traceBecomeLeader(r *raft) {
}

func traceChangeConfEvent(_ pb.ConfChangeI, r *raft) {
}

func traceConfChangeEvent(cfg tracker.Config, r *raft) {
}

func traceBootstrap(r *raft, e ...pb.Entry) {
}

func traceSendMessage(r *raft, m *pb.Message) {
	switch m.Type {
	case pb.MsgHup, pb.MsgBeat, pb.MsgProp, pb.MsgPreVote, pb.MsgPreVoteResp,
		pb.MsgStorageAppend, pb.MsgStorageAppendResp, pb.MsgStorageApply, pb.MsgStorageApplyResp,
		pb.MsgReadIndex, pb.MsgReadIndexResp, pb.MsgUnreachable, pb.MsgCheckQuorum:
		// ignore these messages
		return
	case pb.MsgVote, pb.MsgVoteResp, pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgApp, pb.MsgAppResp:
		r.traceLogger.getEllsbergState().processIncoming(m)
	default:
		r.logger.Errorf("%d validator: unknown message type {%+v}", r.id, m)
	}
}

func traceReceiveMessage(r *raft, m *pb.Message) {
	switch m.Type {
	case pb.MsgHup, pb.MsgBeat, pb.MsgProp, pb.MsgPreVote, pb.MsgPreVoteResp,
		pb.MsgStorageAppend, pb.MsgStorageAppendResp, pb.MsgStorageApply, pb.MsgStorageApplyResp,
		pb.MsgReadIndex, pb.MsgReadIndexResp, pb.MsgUnreachable, pb.MsgCheckQuorum:
		// ignore these messages
		return
	case pb.MsgVote, pb.MsgVoteResp, pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgApp, pb.MsgAppResp:
		r.traceLogger.getEllsbergState().processOutgoing(m)
	default:
		r.logger.Errorf("%d validator: unknown message type {%+v}", r.id, m)
	}
	//r.traceLogger.getValidator().eventC <- event
}
