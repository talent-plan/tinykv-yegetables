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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sync"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	mu   sync.Mutex
	conf *Config
	id   uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	refusedCount     uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raft := &Raft{
		mu:               sync.Mutex{},
		conf:             c,
		id:               c.ID,
		Term:             0,
		Vote:             c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress, 0),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0), //nil
		Lead:             c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		refusedCount:     0,
	}
	raft.initTime(true)
	//rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	log.Debugf("S%d,first log.%v\n", raft.id, raft.RaftLog)

	// fill map
	for _, peer := range c.peers {
		raft.votes[peer] = false
		raft.Prs[peer] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	// clear votes
	raft.clearVote()
	// clear process
	raft.clearProcess(raft.RaftLog.LastIndex())

	state, _, err := c.Storage.InitialState()
	if err == nil {
		raft.Term = state.GetTerm()
		raft.Vote = state.GetVote()
	}
	return raft
}

// becomeFollower transform this peer's state to Follower
func (rf *Raft) becomeFollower(term uint64, lead uint64) {
	rf.initTime(true)
	log.Debugf("S%d %s->follower[%d],leader%d\n", rf.id, rf.State, term, lead)
	rf.State = StateFollower
	rf.Term = term
	rf.Vote = lead
	rf.Lead = lead
	rf.heartbeatElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (rf *Raft) tick() {
	var request *pb.Message
	request = nil
	rf.mu.Lock()
	if rf.State != StateLeader {
		rf.electionElapsed++
		//log.Debug("election:", rf.electionElapsed, "/", rf.electionTimeout)
		if rf.electionElapsed >= rf.electionTimeout {
			rf.initTime(true)
			//开始选举
			//确定term等信息，避免过期
			request = &pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    rf.id,
				To:      rf.id,
				Term:    rf.Term,
				Commit:  rf.RaftLog.committed,
			}
			request.Index = rf.RaftLog.LastIndex()
			request.LogTerm, _ = rf.RaftLog.Term(request.Index)
		}
	} else {
		rf.heartbeatElapsed++
		//log.Debug("heartBeat:", rf.heartbeatElapsed, "/", rf.heartbeatTimeout)
		if rf.heartbeatElapsed >= rf.heartbeatTimeout {
			rf.heartbeatElapsed = 0
			//发送心跳
			request = &pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    rf.id,
				To:      rf.id,
				Term:    rf.Term,
				Entries: nil,
				Commit:  rf.RaftLog.committed,
			}
			// 根据不同的peer匹配Index和LogTerm
		}
	}
	rf.mu.Unlock()
	if request != nil {
		rf.Step(*request)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (rf *Raft) Step(m pb.Message) error {
	//local
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//非Leader可以开始选举
		rf.mu.Lock()
		// 测试忽略term，选举本地消息，我认为需要term
		//if rf.State != StateLeader && rf.Term == m.Term {
		if rf.State != StateLeader {
			rf.becomeCandidate()
			if rf.State == StateCandidate {
				rf.sendVoteAll(m)
			} else if rf.State == StateLeader {
				requet := pb.Message{
					MsgType: pb.MessageType_MsgBeat,
					From:    rf.id,
					To:      rf.id,
					Term:    rf.Term,
					Entries: nil,
					Commit:  rf.RaftLog.committed,
				}
				rf.mu.Unlock()
				defer rf.Step(requet)
				return nil
			}
		} else {
			log.Debugf("S%d[%d] deal with old term%d local-MsgHup,now[%v]\n", rf.id, rf.Term, m.Term, rf.State)
		}
		rf.mu.Unlock()
		return nil
	case pb.MessageType_MsgBeat:
		rf.sendHeartbeatAll(m)
		return nil
	default:

	}

	// remote
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		rf.handleVoteRequest(m)
		return nil
	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
		return nil
	case pb.MessageType_MsgAppend:
		rf.handleAppendEntries(m)
		return nil
	}
	rf.mu.Lock()
	state := rf.State
	rf.mu.Unlock()
	switch state {
	case StateFollower:
		return nil

	case StateCandidate:
		//只有候选人处理 请求投票的回复u
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			rf.voteRequestResponseHandler(m)
		}
		return nil
	case StateLeader:
		//只有leader 接收客户端数据请求
		if m.MsgType == pb.MessageType_MsgPropose {
			rf.clientDataRequest(m)
			return nil

		}
		//leader处理append请求回复
		if m.MsgType == pb.MessageType_MsgAppendResponse {
			rf.appendRequestResoponseHandler(m)
			return nil

		}
		// leader处理心跳回复
		if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			rf.heartBeatResponseHander(m)
			return nil
		}

	}
	log.Debugf("S%d not catch msg%+v\n", m.To, m)
	return nil
}

// handleSnapshot handle Snapshot RPC request
func (rf *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (rf *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (rf *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (rf *Raft) initTime(b bool) {
	rf.electionElapsed = 0
	rf.electionTimeout = rf.conf.ElectionTick + rand.Intn(rf.conf.ElectionTick)
}
func (rf *Raft) clearVote() {
	for _, k := range rf.conf.peers {
		rf.votes[k] = false
		//if k == rf.id {
		//	rf.votes[k] = true
		//}
	}
	rf.refusedCount = 0
}
func (rf *Raft) clearProcess(lastIndex uint64) {
	for k := range rf.Prs {
		rf.Prs[k].Match = 0
		rf.Prs[k].Next = lastIndex + 1
		if k == rf.id {
			rf.Prs[k].Match = lastIndex
		}
	}
	// init   match 0   ,next last+1
	// me   match last,next last+1
}
