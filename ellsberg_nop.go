//go:build !with_ellsberg

package raft

import (
	pb "go.etcd.io/raft/v3/raftpb"
)

type EllsbergState struct {
}

func EllsbergInit(id uint64, logger Logger) EllsbergState {
	return EllsbergState{}
}

func (s *EllsbergState) MainLoop() {
}

func (s *EllsbergState) ReceiveMessage(m *pb.Message) {
}

func (s *EllsbergState) SendMessage(m *pb.Message) {
}
