//go:build with_ellsberg

package raft

import (
	"reflect"

	pb "go.etcd.io/raft/v3/raftpb"
)

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
}

func raftInit() raftState {
	panic("todo")
}

func (s *raftState) equal(t *raftState) bool {
	panic("todo")
}

func (s *raftState) hash() uint64 {
	panic("todo")
}

func (s *raftState) apply(m *pb.Message) raftState {
	panic("todo")
}

func (s *raftState) timeout() raftState {
	panic("todo")
}

func (s *raftState) canApplyAsap(m *pb.Message) bool {
	panic("todo")
}

type partialRaftState struct {
}

func (s *partialRaftState) contains(t *raftState) bool {
	panic("todo")
}

func inferInducing(m *pb.Message) partialRaftState {
	panic("todo")
}

func maybeReachable(s *raftState, pending set[*pb.Message], m *pb.Message, t *partialRaftState) bool {
	panic("todo")
}

func maybeReachableTimeout(s *raftState, pending set[*pb.Message], t *partialRaftState) bool {
	panic("todo")
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
			s.state = s.state.apply(m)
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
	for !q.empty() {
		ss := q.dequeue()
		explored[ss.state.hash()] = append(explored[ss.state.hash()], ss)
		if t.contains(&ss.state) {
			newInstances.add(ss)
			continue
		}
		for m := range ss.pendings {
			if maybeReachable(&ss.state, ss.pendings, m, t) {
				newss := simState{state: ss.state.apply(m), pendings: ss.pendings.copy()}
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
			newss := simState{state: ss.state.timeout(), pendings: ss.pendings.copy()}
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

func EllsbergInit(id uint64, logger Logger) EllsbergState {
	// TODO: should we use buffered channels?
	incomingMsgC := make(chan *pb.Message)
	outgoingMsgC := make(chan *pb.Message)
	instances := make(set[*simState])
	instances.add(&simState{state: raftInit()})
	return EllsbergState{
		instances:    instances,
		incomingMsgC: incomingMsgC,
		outgoingMsgC: outgoingMsgC,
		logger:       logger,
	}
}

func (s *EllsbergState) MainLoop() {
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

func (s *EllsbergState) ReceiveMessage(m *pb.Message) {
	s.logger.Infof("%d ellsberg: incoming msg %+v", s.id, m)
	s.incomingMsgC <- m
}

func (s *EllsbergState) SendMessage(m *pb.Message) {
	s.logger.Infof("%d ellsberg: outgoing msg %+v", s.id, m)
	s.outgoingMsgC <- m
}

func (s *EllsbergState) processIncoming(m *pb.Message) {
	for ss := range s.instances {
		if ss.state.canApplyAsap(m) {
			ss.state = ss.state.apply(m)
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
