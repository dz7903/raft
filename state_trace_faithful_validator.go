//go:build with_faithful_validator

package raft

import (
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"slices"
)

type raftRole int

const (
	Leader raftRole = iota
	Candidate
	Follower
)

type raftEventType int

const (
	eventDisable raftEventType = iota
	eventRecover
	eventApplyConf
	eventBootstrap
	eventTimeout
	eventCommit
	eventReplicate
	eventBecomeLeader
	eventBecomeFollower
	eventSendMessage
	eventReceiveMessage
)

type raftEvent struct {
	eventType raftEventType
	message   *pb.Message
	entries   []pb.Entry
	config    *tracker.Config
	term      uint64
	vote      uint64
	commit    uint64
}

type Validator struct {
	id    uint64
	nodes map[uint64]struct{}
	// server vars
	role raftRole
	term uint64
	vote uint64
	// candidate vars
	votesResponded map[uint64]struct{}
	votesGranted   map[uint64]struct{}
	// leader vars
	matchIndex map[uint64]uint64
	// log vars
	log         []pb.Entry
	commitIndex uint64
	// others
	latestMsg *pb.Message
	disabled  bool
	eventC    chan raftEvent
	logger    Logger
}

func (v *Validator) lastTerm() uint64 {
	if len(v.log) == 0 {
		return 0
	} else {
		return v.log[len(v.log)-1].Term
	}
}

func (v *Validator) assert(cond bool) {
	if !cond {
		v.logger.Errorf("%d: voliation found", v.id)
		panic("violation found")
	}
}

func (v *Validator) assertf(cond bool, format string, args ...any) {
	if !cond {
		v.logger.Errorf("%d violation found: %s", v.id, fmt.Sprintf(format, args...))
		panic("violation found")
	}
}

func (v *Validator) sendAppendEntries(m *pb.Message, isHeartbeat bool) {
	to := m.To
	v.assertf(v.role == Leader, "only Leader can send AppendEntries")
	v.assertf(v.id != to, "can not send AppendEntries to self")
	v.assert(m.Term == v.term)
	// m.Index is "prevLogIndex"
	if m.Index > 0 && m.Index <= uint64(len(v.log)) {
		v.assert(m.LogTerm == v.log[m.Index-1].Term)
	} else {
		v.assert(m.LogTerm == 0)
	}
	v.assert(m.Index+uint64(len(m.Entries)) <= uint64(len(v.log)))
	for i, e := range m.Entries {
		// we do not check the content of entries, only indexes and terms
		v.assertf(e.Index == v.log[m.Index+uint64(i)].Index && e.Term == v.log[m.Index+uint64(i)].Term, "send AppendEntries mismatch: entry %d ({%+v}) in AppendEntries is different from entry %d ({%+v}) in log", i, e, m.Index+uint64(i), v.log[m.Index+uint64(i)])
	}
	if isHeartbeat {
		v.assertf(m.Index == 0 && len(m.Entries) == 0, "hearbeat must be range [0, 0]")
		v.assert(m.Commit == min(v.commitIndex, v.matchIndex[to]))
	} else {
		v.assertf(m.Commit == min(v.commitIndex, m.Index+uint64(len(m.Entries))), "send AppendEntries mismatch: commitIndex=%d log={%+v} message={%+v}", v.commitIndex, v.log, m)
	}
	if m.Context != nil {
		v.assertf(v.log[v.commitIndex-1].Term >= v.term, "readIndex violation: can not attach readIndex when a leader haven't commit an entry in its own term (commitIndex=%d, commitTerm=%d, currentTerm=%d)", v.commitIndex, v.log[v.commitIndex-1].Term, v.term)
	}
}

func (v *Validator) sendAppendEntriesResp(m *pb.Message, isHeartbeat bool) {
	if isHeartbeat {
		v.assert(v.latestMsg != nil && v.latestMsg.Type == pb.MsgHeartbeat)
	} else {
		v.assert(v.latestMsg != nil && v.latestMsg.Type == pb.MsgApp)
	}
	m1 := v.latestMsg
	v.assert(m1.Term <= v.term)
	// m1.Index is "prevLogIndex", m1.LogTerm is "prevLogTerm"
	logOk := m1.Index == 0 || m1.Index > 0 && m1.Index <= uint64(len(v.log)) && m1.LogTerm == v.log[m1.Index-1].Term
	if m.Reject {
		v.assert(m1.Term < v.term || m1.Term == v.term && v.role == Follower && !logOk)
		v.assert(m.Term == v.term)
		v.assert(m.Index == 0)
	} else {
		v.assert(v.role == Follower)
		v.assertf(m1.Term == v.term, "term mismatch: currentTerm=%d, msg={%+v}", v.term, m1)
		v.assertf(logOk, "log not OK, should not send AppendEntriesResp, currentLog={%+v}", v.log)
		if m1.Index < v.commitIndex {
			if isHeartbeat {
				v.commitIndex = max(v.commitIndex, m1.Commit)
				v.logger.Infof("%d update commitIndex=%d", v.id, v.commitIndex)
			}
		} else if len(m1.Entries) > 0 {
			firstConflictIndex := 0
			for firstConflictIndex < len(m1.Entries) && m1.Index+uint64(firstConflictIndex) < uint64(len(v.log)) {
				if m1.Entries[firstConflictIndex].Term != v.log[m1.Index+uint64(firstConflictIndex)].Term {
					v.logger.Warningf("%d conflict detected at index %d (expected {%+v}, found {%+v})", v.id, m1.Index+uint64(firstConflictIndex)+1, m1.Entries[firstConflictIndex], v.log[m1.Index+uint64(firstConflictIndex)])
					break
				}
				firstConflictIndex++
			}
			v.log = v.log[0 : m1.Index+uint64(firstConflictIndex)]
			v.log = append(v.log, m1.Entries[firstConflictIndex:]...)
			v.commitIndex = max(v.commitIndex, min(m1.Commit, m1.Index+uint64(len(m1.Entries))))
		}
		v.assert(m.Term == v.term)
		if isHeartbeat || m1.Index >= v.commitIndex {
			v.assert(m.Index == m1.Index+uint64(len(m1.Entries)))
		} else {
			v.assert(m.Index == v.commitIndex)
		}
	}
	v.latestMsg = nil
}

func (v *Validator) receiveAppendEntriesResp(m *pb.Message, _ bool) {
	from := m.From
	// TLA+ specification does not assert node to be Leader
	v.assert(m.Term == v.term)
	if !m.Reject {
		v.matchIndex[from] = max(v.matchIndex[from], m.Index)
	}
}

// There are 4 types of transition:
//  1. no message in, no message out (Timeout, BecomeLeader, Commit, Replicate)
//     in this case we have "hints".
//  2. no message in, one message out (RequestVote, AppendEntries)
//     in this case we check when sending the message.
//  3. one message in, no message out (BecomeFollower, HandleRequestVoteResp, HandleAppendEntriesResp)
//     in this case we check when receiving the message.
//  4. one message in, one message out (HandleRequestVote, HandleAppendEntries [without ReturnToFollowerState])
//     in this case we first store the received message as `latestMsg`, and check when sending the message.
func (v *Validator) mainLoop() {
	for {
		event := <-v.eventC
		if v.disabled {
			continue
		}
		switch event.eventType {
		case eventDisable:
			v.logger.Infof("%d disabling trace validator", v.id)
			v.disabled = true
		case eventRecover:
			v.logger.Infof("%d event recover hard state term=%d vote=%d commit=%d log={%+v}", v.id, event.term, event.vote, event.commit, event.entries)
			v.role = Follower
			v.term = event.term
			v.vote = event.vote
			v.commitIndex = event.commit
			v.log = event.entries
		case eventApplyConf:
			// we don't check conf change; it's only used for updating information
			v.logger.Infof("%d event apply new config {%+v}", v.id, event.config)
			v.nodes = event.config.Voters.IDs()
		case eventBootstrap:
			// when bootstrap, Follower is allowed to put in some ConfChange entries into log
			v.logger.Infof("%d event bootstrap with logs {%+v}", v.id, event.entries)
			for _, e := range event.entries {
				v.assert(e.Type == pb.EntryConfChange)
			}
			v.assert(len(v.log) == 0)
			v.log = event.entries
			// the bootstrapped entries are committed
			v.commitIndex = uint64(len(event.entries))
		case eventTimeout:
			v.logger.Infof("%d event timeout", v.id)
			v.assert(v.role == Follower || v.role == Candidate)
			v.role = Candidate
			v.term = v.term + 1
			v.logger.Infof("%d current term is %d", v.id, v.term)
			v.vote = v.id
			v.votesResponded = make(map[uint64]struct{})
			v.votesGranted = make(map[uint64]struct{})
		case eventBecomeLeader:
			v.logger.Infof("%d event become leader", v.id)
			v.assert(v.role == Candidate)
			v.assertf(len(v.votesGranted)*2 > len(v.nodes), "%d only %d votes, but there are %d nodes", v.id, len(v.votesGranted), len(v.nodes))
			v.role = Leader
			v.matchIndex = make(map[uint64]uint64)
			for i := range v.nodes {
				if i == v.id {
					v.matchIndex[i] = uint64(len(v.log))
				} else {
					v.matchIndex[i] = 0
				}
			}
		case eventBecomeFollower:
			v.logger.Infof("%d event become follower to term %d", v.id, event.term)
			v.assertf(v.term <= event.term, "becomeFollower should not decrease the term")
			if v.term < event.term {
				v.role = Follower
				v.term = event.term
				v.vote = 0
			} else {
				v.logger.Infof("%d role=%d step down to follower", v.id, v.role)
				v.role = Follower
				v.term = event.term
			}
		case eventCommit:
			v.logger.Infof("%d event commit", v.id)
			v.assertf(v.role == Leader, "only leader can commit")
			v.logger.Infof("%d match index when commit: {%+v}", v.id, v.matchIndex)
			indexes := make([]uint64, 0, len(v.matchIndex))
			for _, index := range v.matchIndex {
				indexes = append(indexes, index)
			}
			slices.Sort(indexes)
			v.commitIndex = max(v.commitIndex, indexes[len(v.matchIndex)/2])
			v.logger.Infof("%d commit to index %d", v.id, v.commitIndex)
		case eventReplicate:
			v.logger.Infof("%d event replicate {%+v}", v.id, event.entries)
			v.assertf(v.role == Leader, "only leader can replicate")
			for i, e := range event.entries {
				v.assertf(e.Term == v.term, "replicate must be the same term (currentTerm=%d) but term=%d found", v.term, e.Term)
				v.assertf(e.Index == uint64(len(v.log)+i+1), "replicate entry index mismatch, expected %d but %d found (log={%+v})", len(v.log)+i+1, e.Index, v.log)
			}
			v.log = append(v.log, event.entries...)
		case eventSendMessage:
			m := event.message
			v.logger.Infof("%d event send message {%+v}", v.id, m)
			if m.From != v.id {
				v.logger.Warningf("%d sent message mismatch in From", v.id)
				continue
			}
			v.assert(m.From == v.id)
			to := m.To
			switch m.Type {
			case pb.MsgVote:
				v.logger.Infof("%d event send RequestVote to %d", v.id, to)
				v.assertf(v.role == Candidate, "only Candidate can send RequestVote")
				v.assertf(v.id != to, "can't send RequestVote to self")
			case pb.MsgVoteResp:
				v.logger.Infof("%d event send RequestVoteResp to %d", v.id, to)
				if to == v.id {
					v.assertf(v.role == Candidate, "only Candidate can send RequestVoteResp to itself")
					v.assertf(v.term == event.message.Term, "send RequestVoteResp term mismatch (expected %d, found %d)", event.message.Term, v.term)
					v.assert(!event.message.Reject)
				} else {
					v.assertf(v.latestMsg != nil && v.latestMsg.Type == pb.MsgVote, "expect RequestVote received")
					m1 := v.latestMsg
					v.assert(m1.From == m.To)
					// m1.Index >= lastIndex = len(v.log)-1
					logOk := m1.LogTerm > v.lastTerm() || m1.LogTerm == v.lastTerm() && m1.Index+1 >= uint64(len(v.log))
					grant := logOk && m1.Term == v.term && (v.vote == None || v.vote == m1.From)
					v.assertf(m.Term == v.term, "send RequestVoteResp term mismatch, expected %d but %d found", v.term, m.Term)
					if m.Reject {
						v.assertf(!grant, "send RequestVoteResp mismatch, should reject (logOk=%t, shouldGrant=%t, lastTerm=%d, logLen=%d, term=%d, vote=%d, msgTerm=%d)", logOk, grant, v.lastTerm(), len(v.log), v.term, v.vote, m1.Term)
					} else {
						v.assertf(grant, "send RequestVoteResp mismatch, should grant (logOk=%t, shouldGrant=%t, lastTerm=%d, logLen=%d, term=%d, vote=%d, msgTerm=%d)", logOk, grant, v.lastTerm(), len(v.log), v.term, v.vote, m1.Term)
						v.vote = m1.From
					}
					v.latestMsg = nil
				}
			case pb.MsgHeartbeat:
				v.logger.Infof("%d event send Heartbeat to %d", v.id, to)
				v.sendAppendEntries(m, true)
			case pb.MsgHeartbeatResp:
				v.logger.Infof("%d event send HeartbeatResp to %d", v.id, to)
				v.sendAppendEntriesResp(m, true)
			case pb.MsgApp:
				v.logger.Infof("%d event send AppendEntries to %d", v.id, to)
				v.sendAppendEntries(m, false)
			case pb.MsgAppResp:
				v.logger.Infof("%d event send AppendEntriesResp to %d", v.id, to)
				if v.id == m.To {
					// special case: leader may send AppendEntriesResp to itself
					v.assert(v.role == Leader)
					v.assert(m.Term == v.term)
					v.assert(!m.Reject)
					// m.Index is "matchIndex"
					// Note: etcd-raft log starts from index 1, so len(v.log) == lastIndex here
					v.assertf(m.Index == uint64(len(v.log)), "AppendEntriesResp to self index mismatch, expected %d but %d found (log={%+v})", len(v.log), m.Index, v.log)
				} else {
					v.sendAppendEntriesResp(m, false)
				}
			default:
				v.logger.Errorf("%d validator: unknown message type {%+v}", v.id, m)
				panic("unknown message type")
			}
		case eventReceiveMessage:
			m := event.message
			v.logger.Infof("%d event receive message {%+v}", v.id, m)
			if m.To != v.id {
				v.logger.Warningf("%d received message mismatch in To", v.id)
				continue
			}
			// v.assert(m.To == v.id)
			from := m.From
			if m.Term > v.term {
				v.logger.Infof("%d event become follower by msg", v.id)
				v.role = Follower
				v.term = m.Term
				v.vote = 0
				// continue to process this message
			}
			switch m.Type {
			case pb.MsgVote:
				v.logger.Infof("%d event receive RequestVote from %d", v.id, from)
				v.assertf(v.latestMsg == nil, "message {%+v} not processed immediately", v.latestMsg)
				v.latestMsg = event.message
			case pb.MsgVoteResp:
				v.logger.Infof("%d event receive RequestVoteResp from %d", v.id, from)
				if m.Term < v.term {
					// DropStaleResponse
					v.logger.Infof("%d event drop stale response {%+v}", v.id, m)
				} else {
					v.assert(m.Term == v.term)
					v.votesResponded[from] = struct{}{}
					if !m.Reject {
						v.votesGranted[from] = struct{}{}
					}
				}
			case pb.MsgHeartbeat:
				v.logger.Infof("%d event receive Heartbeat from %d", v.id, from)
				v.assert(event.message.Term <= v.term)
				if event.message.Term == v.term && v.role == Candidate {
					v.logger.Infof("%d event ReturnToFollowerState", v.id)
					v.role = Follower
				} else {
					v.assertf(v.latestMsg == nil, "message {%+v} not processed immediately", v.latestMsg)
					v.latestMsg = event.message
				}
			case pb.MsgHeartbeatResp:
				v.logger.Infof("%d event receive HeartbeatResp from %d", v.id, from)
				if m.Term < v.term {
					// DropStaleResponse
					v.logger.Infof("%d event drop stale response {%+v}", v.id, m)
				} else {
					v.receiveAppendEntriesResp(m, true)
				}
			case pb.MsgApp:
				v.logger.Infof("%d event receive AppendEntries from %d", v.id, from)
				v.assert(event.message.Term <= v.term)
				if event.message.Term == v.term && v.role == Candidate {
					v.logger.Infof("%d event ReturnToFollowerState", v.id)
					v.role = Follower
				} else {
					v.assertf(v.latestMsg == nil, "message {%+v} not processed immediately", v.latestMsg)
					v.latestMsg = event.message
				}
			case pb.MsgAppResp:
				v.logger.Infof("%d event receive AppendEntriesResp from %d", v.id, from)
				if m.Term < v.term {
					// DropStaleResponse
					v.logger.Infof("%d event drop stale response {%+v}", v.id, m)
				} else {
					v.receiveAppendEntriesResp(m, false)
				}
			default:
				v.logger.Errorf("%d validator: unknown message type {%+v}", v.id, m)
				panic("unknown message type")
			}
		}
	}
}

type TraceLogger interface {
	getValidator() *Validator
}

type MyTraceLogger struct {
	validator Validator
}

const CHANNEL_BUFFER_SIZE int = 0

func (l *MyTraceLogger) getValidator() *Validator {
	return &l.validator
}

func traceInitState(r *raft) {
	r.logger.Infof("%d event init state", r.id)
	if r.traceLogger != nil {
		r.logger.Warningf("%d user supplied validator is ignored")
	}
	v := Validator{
		id:             r.id,
		nodes:          r.trk.Voters.IDs(),
		term:           r.Term,
		role:           Follower,
		vote:           r.Vote,
		votesResponded: make(map[uint64]struct{}),
		votesGranted:   make(map[uint64]struct{}),
		log:            r.raftLog.allEntries(),
		disabled:       false,
		eventC:         make(chan raftEvent, CHANNEL_BUFFER_SIZE),
		logger:         r.logger,
	}
	r.traceLogger = &MyTraceLogger{v}
	go v.mainLoop()
}

func traceRecoverState(r *raft) {
	r.logger.Infof("%d event recover state", r.id)
	r.logger.Infof("%d actual log={%+v}", r.id, r.raftLog.allEntries())
	// make "fake" entries for compacted
	entries := make([]pb.Entry, r.raftLog.firstIndex()-1)
	for i := range entries {
		entries[i].Index = uint64(i) + 1
		term, err := r.raftLog.term(uint64(i) + 1)
		if err == nil {
			entries[i].Term = term
		}
	}
	entries = append(entries, r.raftLog.allEntries()...)
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventRecover, term: r.Term, vote: r.Vote, commit: r.raftLog.committed, entries: entries}
}

func traceReady(*raft) {}

func traceCommit(r *raft) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventCommit}
}

func traceReplicate(r *raft, e ...pb.Entry) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventReplicate, entries: e}
}

func traceBecomeFollower(r *raft) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventBecomeFollower, term: r.Term}
}

func traceBecomeCandidate(r *raft) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventTimeout}
}

func traceBecomeLeader(r *raft) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventBecomeLeader}
}

func traceChangeConfEvent(_ pb.ConfChangeI, r *raft) {
	r.logger.Errorf("%d validator: change conf event not supported", r.id)
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventDisable}
}

func traceConfChangeEvent(cfg tracker.Config, r *raft) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventApplyConf, config: &cfg}
}

func traceBootstrap(r *raft, e ...pb.Entry) {
	r.traceLogger.getValidator().eventC <- raftEvent{eventType: eventBootstrap, entries: e}
}

func shouldIgnore(mt pb.MessageType) bool {
	switch mt {
	case pb.MsgHup, pb.MsgBeat, pb.MsgProp, pb.MsgPreVote, pb.MsgPreVoteResp,
		pb.MsgStorageAppend, pb.MsgStorageAppendResp, pb.MsgStorageApply, pb.MsgStorageApplyResp,
		pb.MsgReadIndex, pb.MsgReadIndexResp,
		pb.MsgUnreachable:
		return true
	default:
		return false
	}
}

func traceSendMessage(r *raft, m *pb.Message) {
	if shouldIgnore(m.Type) {
		return
	}
	event := raftEvent{eventType: eventSendMessage, message: m}
	r.traceLogger.getValidator().eventC <- event
}

func traceReceiveMessage(r *raft, m *pb.Message) {
	if shouldIgnore(m.Type) {
		return
	}
	event := raftEvent{eventType: eventReceiveMessage, message: m}
	r.traceLogger.getValidator().eventC <- event
}
