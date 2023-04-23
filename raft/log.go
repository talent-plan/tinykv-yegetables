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
	"strconv"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//applied,commited,lastIndex-->都是开区间
	//Storage保存了如下信息。
	//HardState
	//---Term：即Raft.Term
	//---Vote：即Raft.Vote
	//---Commit：即Raft.RaftLog.committed
	//Snapshot
	//---Data：即Snapshot中所有的Entry
	//---MetaData
	//------Index：Snapshot最后一个Entry的Index
	//------Term：Snapshot最后一个Entry的Term
	//------ConfState
	//---------Nodes：即Peers
	//Stabled Entries：即ms.ents

	// soft state
	// ---- lead
	// ---- raftstate
	//
	//作者：李素晴
	//链接：https://juejin.cn/post/7040086340154687518
	//来源：稀土掘金
	//著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
	raftlog := &RaftLog{
		storage:   storage,
		committed: 0,
		stabled:   0,
		applied:   0,
		//entries:         make([]pb.Entry, 0),
		pendingSnapshot: nil,
	}
	state, _, err := storage.InitialState()
	if err == nil {
		raftlog.committed = state.Commit
	}
	first, err := storage.FirstIndex()
	if err == nil {
		raftlog.applied = first - 1
	}
	last, err := storage.LastIndex()
	if err == nil {
		raftlog.stabled = last
	}
	raftlog.entries, _ = storage.Entries(first, last+1)
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.getLogRange(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.getLogRange(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].GetIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].GetIndex() == index {
			return l.entries[i].GetTerm(), nil
		}
	}

	return 0, errors.New("not fount index" + strconv.Itoa(int(index)))
}
func (l *RaftLog) FirstLog() (uint64, uint64) {
	if len(l.entries) == 0 {
		return 0, 0
	}
	return l.entries[0].GetIndex(), l.entries[0].GetTerm()
}
func (l *RaftLog) FirstLogIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].GetIndex()
}

// [)
func (l *RaftLog) getLogRange(start, end uint64) []pb.Entry {
	results := make([]pb.Entry, 0)
	for _, e := range l.entries {
		index := e.GetIndex()
		if index >= end {
			break
		}
		if index >= start {
			results = append(results, e)
		}
	}
	return results
}
func (l *RaftLog) FirstTermIndex(term uint64) uint64 {
	lastIndex := 0
	for i := 0; i < len(l.entries); i++ {
		if l.entries[i].Term == term {
			return l.entries[i].Index //任期的最小索引
		}
		if l.entries[i].Term < term {
			lastIndex = int(l.entries[i].Index)
		}
	}
	return uint64(lastIndex)
}
func (l *RaftLog) getLogLastIndexAndTerm() (uint64, uint64) {
	index := l.LastIndex()
	term, _ := l.Term(index)
	return index, term
}

func (l *RaftLog) getLog(index uint64) *pb.Entry {
	for i := len(l.entries) - 1; i >= 0; i-- {
		entry := l.entries[i]
		if entry.Index == index {
			return &l.entries[i]
		}
	}
	return nil
}
