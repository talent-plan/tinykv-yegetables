// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	Config    *Config
	lastReady Ready
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	rn := &RawNode{
		Raft:   newRaft(config),
		Config: config,
	}
	lastready, _ := rn.fillReady(false)
	rn.lastReady = *lastready
	return rn, nil
}

func (rn *RawNode) fillReady(diff bool) (*Ready, bool) {
	rf := rn.Raft
	isDiff := false
	ready := &Ready{}
	s := rn.getSoft()
	h := *rn.getHard()
	if diff && rn.lastReady.HardState.Term == h.Term && rn.lastReady.HardState.Vote == h.Vote && rn.lastReady.HardState.Commit == h.Commit {
		ready.HardState = pb.HardState{}
	} else {
		ready.HardState = h
		if diff {
			isDiff = true
		}
	}

	if diff && rn.lastReady.SoftState != nil && rn.lastReady.SoftState.Lead == s.Lead && rn.lastReady.SoftState.RaftState == s.RaftState {
		ready.SoftState = nil
	} else {
		ready.SoftState = s
		if diff {
			isDiff = true
		}
	}
	ready.Messages = nil
	if len(rf.msgs) != 0 {
		ready.Messages = make([]pb.Message, 0)
		ready.Messages = append(ready.Messages, rf.msgs...)
	}

	ready.Entries = rf.RaftLog.unstableEntries()
	ready.CommittedEntries = rf.RaftLog.nextEnts()
	return ready, isDiff
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

func (rn *RawNode) getSoft() *SoftState {
	r := rn.Raft
	nowSoft := &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
	// if rn.lastReady.SoftState != nil {
	// 	if rn.lastReady.SoftState.Lead == nowSoft.Lead && rn.lastReady.SoftState.RaftState == nowSoft.RaftState {
	// 		return nil
	// 	}
	// }
	// rn.lastReady.SoftState = nowSoft
	return nowSoft
}

func (rn *RawNode) getHard() *pb.HardState {
	r := rn.Raft
	nowHard := &pb.HardState{
		Term:                 r.Term,
		Vote:                 r.Vote,
		Commit:               r.RaftLog.committed,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	// if rn.lastReady.HardState.Term == nowHard.Term && rn.lastReady.HardState.Vote == nowHard.Vote && rn.lastReady.HardState.Commit == nowHard.Commit {
	// 	return nil
	// }
	// rn.lastReady.HardState = *nowHard
	return nowHard
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	rf := rn.Raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ready, _ := rn.fillReady(true)
	return *ready

}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	rf := rn.Raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, diff := rn.fillReady(true)
	return !diff
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	rf := rn.Raft
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := rn.Raft
	g := r.RaftLog
	for _, v := range rd.Messages {
		rn.Step(v)
	}
	g.stabled = g.LastIndex()
	g.applied = g.committed

	ready, _ := rn.fillReady(true)
	rn.lastReady = *ready

	// Your Code Here (2A).
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
