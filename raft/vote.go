package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// becomeCandidate transform this peer's state to candidate
// Your Code Here (2A).
func (rf *Raft) becomeCandidate() {
	if rf.State == StateLeader {
		return
	}
	log.Debugf("S%d[t%d] %s->becomeCandidate \n", rf.id, rf.Term+1, rf.State)
	rf.Term++
	rf.State = StateCandidate
	rf.Vote = rf.id

	// clear votes
	rf.clearVote()
	rf.votes[rf.id] = true
	if rf.winVote() {
		rf.becomeLeader()
		log.Debugf("S%d [%d] become Leader(win becomeCandidate)\n", rf.id, rf.Term)
	}
}

func (rf *Raft) sendVoteAll(m pb.Message) {
	log.Debugf("S%d[%d] sendVoteRequest All\n", rf.id, rf.Term)
	//if rf.State != StateCandidate || m.Term != rf.Term-1 {
	// 测试忽略term，选举本地消息，我认为需要term

	if rf.State != StateCandidate {
		return
	}
	m.Term = rf.Term
	m.MsgType = pb.MessageType_MsgRequestVote
	m.Index = rf.RaftLog.LastIndex()
	m.LogTerm, _ = rf.RaftLog.Term(m.Index)
	for peer := range rf.votes {
		if peer == m.From {
			continue
		}
		m.To = peer
		rf.msgs = append(rf.msgs, m)
	}
}

func (rf *Raft) handleVoteRequest(request pb.Message) {
	rf.mu.Lock()
	reply := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      request.From,
		From:    rf.id,
		Term:    rf.Term,
		Reject:  true,
	}
	defer func() {
		rf.msgs = append(rf.msgs, *reply)
		rf.mu.Unlock()
	}()

	if request.Term < rf.Term {
		return
	}
	//if request.Term == rf.Term {
	//	if rf.State == StateLeader {
	//		log.Debugf("S%d :S%d request vote.refused args={%+v},me={vote%d,role=%s,term=%v}\n", rf.id, request.From, request, rf.Vote, rf.State, rf.Term)
	//		return
	//	}
	//}
	// 任期大,切换follower,更新日期.重新等待投票
	if request.Term > rf.Term {
		// 请求投票时不知到leader是谁，默认为自己
		rf.becomeFollower(request.Term, 0)
		reply.Term = request.Term
		log.Debugf("S%d :become follower to S%d(term>me) [agree] me{vote-for=%d,term=%d}\n", rf.id, request.From, rf.Vote, rf.Term)
	}
	lastIndex := rf.RaftLog.LastIndex()
	lastTerm, _ := rf.RaftLog.Term(lastIndex)
	noVoted := rf.Vote == 0 || rf.Vote == request.GetFrom()
	if noVoted {
		if lastTerm < request.GetLogTerm() || (lastTerm == request.GetLogTerm() && lastIndex <= request.GetIndex()) {
			// vote
			rf.initTime(true)
			reply.Reject = false
			rf.Vote = request.GetFrom()
			log.Debugf("S%d agree vote to S%d\n", rf.id, request.From)
		} else {
			log.Debugf("S%d refused vote to S%d. prevlog not match req=[%d-%d],me[%d-%d]\n", rf.id, request.From, request.Index, request.LogTerm, lastIndex, lastTerm)
		}
	} else {
		log.Debugf("S%d refused vote to S%d.voted %d\n", rf.id, request.From, rf.Vote)
	}
}

func (rf *Raft) voteRequestResponseHandler(response pb.Message) {
	rf.mu.Lock()
	if rf.Term != response.Term || rf.State != StateCandidate {
		//Debug(dVote, "S%d S%d agree vote but old[before-%d,now-%d]", me, peer, request.Term, rf.term)
		rf.mu.Unlock()
		return
	}
	rf.votes[response.From] = !response.Reject

	if response.Reject == false {
		log.Debugf("S%d[%d] S%d agree vote\n", rf.id, rf.Term, response.From)
	} else {
		rf.refusedCount++
		if response.Term > rf.Term {
			rf.initTime(true)
			rf.becomeFollower(response.Term, rf.id)
			log.Debugf("S%d S%d{t=%d>met%d},refused\n", rf.id, response.From, response.Term, rf.Term)
			rf.mu.Unlock()
			return
		}
		log.Debugf("S%d S%d{t=%d},refused\n", rf.id, response.From, response.Term)
	}

	if rf.winVote() {
		rf.becomeLeader()
		request := pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    rf.id,
			To:      rf.id,
			Term:    rf.Term,
			Entries: nil,
			Commit:  rf.RaftLog.committed,
		}
		rf.mu.Unlock()
		defer rf.Step(request)
		return
	} else {
		log.Debugf("S%d not yet win vote %d\n", rf.id, rf.Term)
	}
	rf.mu.Unlock()
}

// becomeLeader transform this peer's state to leader
func (rf *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if rf.State != StateLeader {
		rf.State = StateLeader
		rf.initTime(true)
		rf.Vote = rf.id
		lastIndex := rf.RaftLog.LastIndex()
		rf.clearProcess(lastIndex)
		entry := pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      rf.Term,
			Index:     lastIndex + 1,
			Data:      nil,
		}
		rf.RaftLog.entries = append(rf.RaftLog.entries, entry)
		rf.updateCommit()
		log.Debugf("S%d becomeLeader[%d].addnooplog.%+v\n", rf.id, rf.Term, rf.RaftLog)
		rf.sendAppendAll()
	}
}
func (rf *Raft) winVote() bool {
	votes := rf.votes
	me := rf.id
	agree := 0
	for id, v := range votes {
		if id == me || v {
			agree++
		}
	}
	if agree > len(votes)/2 {
		return true
	}
	if rf.refusedCount > uint64(len(votes)/2) {
		// become follower becouse half peoper reply&&reject
		log.Debugf("S%d reject vote.half people.->follower[%d]\n", rf.id, rf.Term)
		rf.becomeFollower(rf.Term, 0)
	}
	return false
}
