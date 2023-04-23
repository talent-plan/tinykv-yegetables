package raft

import (
	"math"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (rf *Raft) sendAppend(to uint64) bool {
	if rf.State != StateLeader || to == rf.id {
		return false
	}
	me := rf.id
	request := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    me,
		Term:    rf.Term,
		LogTerm: 0,
		Index:   0,
		Entries: make([]*pb.Entry, 0),
		Commit:  rf.RaftLog.committed,
		//Snapshot: nil,
		//Reject:   false,
	}
	defer func() {
		rf.msgs = append(rf.msgs, *request)
	}()
	lastIndex := rf.RaftLog.LastIndex()
	next := rf.Prs[to].Next
	if next < 1 {
		rf.Prs[to].Next = rf.Prs[to].Match + 1
		next = rf.Prs[to].Next
	}
	request.Index = next - 1
	request.LogTerm, _ = rf.RaftLog.Term(request.Index)
	earray := rf.RaftLog.getLogRange(next, lastIndex+1)
	for start := 0; start < len(earray); start++ {
		request.Entries = append(request.Entries, &earray[start])
	}
	log.Debugf("S%d appdata becouse[m%d:n%d],so send S%d[prev%d:%d] log=%v\n", rf.id, rf.Prs[to].Match, rf.Prs[to].Next, request.To, request.Index, request.LogTerm, request.Entries)
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (rf *Raft) handleAppendEntries(req pb.Message) {
	reply := &pb.Message{
		MsgType:  pb.MessageType_MsgAppendResponse,
		To:       req.From,
		From:     req.To,
		Term:     0,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   true,
	}
	rf.mu.Lock()
	defer func() {
		rf.msgs = append(rf.msgs, *reply)
		rf.mu.Unlock()
	}()
	{
		// 普通心跳处理
		reply.Term = rf.Term
		if req.Term < rf.Term {
			log.Debugf("S%d S%d 's append,term<me.refused\n", rf.id, req.From)
			return
		}
		rf.initTime(true)
		log.Debugf("S%d S%d 's append-(init time)\n", rf.id, req.From)
		rf.becomeFollower(req.Term, req.From)
		reply.Term = rf.Term
	}

	//Debug(dClient, "S%d S%d sendHeartbeat args=", rf.me, args.LeaderId)
	//appendArgsPrint(args, dClient, rf.me)
	//接收者的实现：
	//返回假 如果领导人的任期小于接收者的当前任期（译者注：这里的接收者是指跟随者或者候选人）（5.1 节）

	//切换follower,更新term. (同意投票)
	//在等待投票期间，candidate 可能会收到另一个声称自己是 leader 的服务器节点发来的 AppendEntries RPC 。
	//如果这个 leader 的任期号（包含在RPC中）不小于 candidate 当前的任期号，那么 candidate 会承认该 leader 的合法地位并回到 follower 状态。
	//如果 RPC 中的任期号比自己的小，那么 candidate 就会拒绝这次的 RPC 并且继续保持 candidate 状态。
	rf.compareLog(req, reply)
}

func (rf *Raft) compareLog(req pb.Message, reply *pb.Message) {
	//返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm
	//匹配上 （译者注：在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
	log.Debugf("S%d compareLog.recv=[%d,%d,c%d]{%v}\n", rf.id, req.GetIndex(), req.LogTerm, req.Commit, req.Entries)
	matchTerm, ok := rf.RaftLog.Term(req.GetIndex())
	if ok == nil && matchTerm == req.GetLogTerm() {
		// 匹配
		reply.Reject = false
		log.Debugf("S%d S%d Heartbeat match log,len%d\n", rf.id, req.From, len(rf.RaftLog.entries))
	} else {
		reply.Reject = true
		reply.Index = req.Index
		reply.LogTerm = matchTerm // 冲突任期
		//reply.Index = rf.RaftLog.FirstTermIndex(matchTerm)
		log.Debugf("S%d S%d Heartbeat conflict log,return %d-%d\n", rf.id, req.From, reply.Index, reply.LogTerm)
		return
	}

	//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
	same := 0
	for _, t := range req.Entries {
		currentTerm, ok := rf.RaftLog.Term(t.Index)
		if ok != nil {
			break
		}
		if t.Term != currentTerm {
			log.Debugf("S%d log lookup error,overwrite %v\n", rf.id, t.Index)
			rf.RaftLog.entries = rf.RaftLog.getLogRange(0, t.Index)
			// rollback stable
			if rf.RaftLog.stabled >= t.Index {
				rf.RaftLog.stabled = t.Index - 1
			}
			break
		} else {
			same++
		}
	}
	//追加日志中尚未存在的任何新条目
	for _, e := range req.Entries {
		if same != 0 {
			same--
			continue
		}
		rf.RaftLog.entries = append(rf.RaftLog.entries, *e)
	}
	lastIndex, lastTerm := rf.RaftLog.getLogLastIndexAndTerm()
	reply.Index = lastIndex
	//logPrint2(rf.log, dLog, rf.me, fmt.Sprintf("copy log. lastlog={%d-%d}", lastIndex, lastTerm))
	log.Debugf("S%d copy log.---ok lastlog={%d-%d}", rf.id, lastIndex, lastTerm)
	log.Debugf("S%d alllog=%v", rf.id, rf.RaftLog.entries)
	//logPrintAll(rf.log, dLog, rf.me, fmt.Sprintf("alllog%d=", len(rf.log)))
	//如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if req.GetCommit() > rf.RaftLog.committed {
		old := rf.RaftLog.committed
		rf.RaftLog.committed = uint64(int(math.Min(float64(req.GetCommit()), float64(req.GetIndex()+uint64(len(req.Entries))))))
		log.Debugf("S%d commit %d -> %d", rf.id, old, rf.RaftLog.committed)
	}
}

func (rf *Raft) clientDataRequest(m pb.Message) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != StateLeader {
		return
	}
	for _, e := range m.Entries {
		e.Term = rf.Term
		e.Index = rf.RaftLog.LastIndex() + 1
		rf.RaftLog.entries = append(rf.RaftLog.entries, *e)
	}
	// if only one no reply
	rf.updateCommit()

	log.Debugf("S%d add entries %v\n", rf.id, m.Entries)
	log.Debugf("S%d now all entries %+v\n", rf.id, rf.RaftLog.allEntries())
	rf.sendAppendAll()
}
func (rf *Raft) sendAppendAll() {
	//Debug(dLeader, "S%d Leader,headBeat to ALL follower", rf.me)
	peers := rf.Prs
	me := rf.id
	for peer := range peers {
		if peer == me {
			continue
		}
		rf.sendAppend(peer)
	}
}

func (rf *Raft) appendRequestResoponseHandler(reply pb.Message) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	{
		if rf.State != StateLeader {
			return
		}
		//Debug(dLeader, "S%d headbeat to S%d result=%v term=%d me{%d}", me, peer, reply.Success, reply.Term, request.Term)
		if reply.Term > rf.Term {
			rf.becomeFollower(reply.Term, None)
			//Debug(dLeader, "S%d old leader become follower{vf=%d,t=%d}", me, reply.VotedFor, reply.Term)
			return
		}
	}
	var newMatch, newNext uint64
	newMatch = rf.Prs[reply.From].Match
	if reply.Reject {
		if reply.LogTerm == 0 {
			// not found index
			newNext = reply.Index
		} else {
			// error logterm
			newNext = rf.RaftLog.FirstTermIndex(reply.GetLogTerm())
			if newNext == 0 {
				newNext = newMatch + 1
				log.Errorf("S%d no found fit term %d index\n", rf.id, reply.GetLogTerm())
			}
		}
	} else {
		newNext = reply.Index + 1
		newMatch = reply.Index
	}
	log.Debugf("S%d appReqResp replyId=%d--updateS%d.match %d->%d,next %d->%d\n", rf.id, reply.Index, reply.From, rf.Prs[reply.From].Match, newMatch, rf.Prs[reply.From].Next, newNext)
	rf.Prs[reply.From].Next = newNext
	rf.Prs[reply.From].Match = newMatch
	if rf.updateCommit() {
		rf.sendAppendAll()
	} else {
		if reply.Reject {
			// retry
			rf.sendAppend(reply.From)
		}
	}
}
func midCommitIndex(prs map[uint64]*Progress) uint64 {
	var args uint64Slice
	for _, p := range prs {
		args = append(args, p.Match)
	}
	sort.Sort(args)
	return args[(len(prs)-1)/2]
}
func (rf *Raft) updateCommit() bool {
	// get 1/2
	// update self
	index, _ := rf.RaftLog.getLogLastIndexAndTerm()
	rf.Prs[rf.id].Match, rf.Prs[rf.id].Next = index, index+1

	temp := midCommitIndex(rf.Prs)
	log.Debugf("S%d appendReqResp--midc=%d,me c=%d\n", rf.id, temp, rf.RaftLog.committed)
	// commit ++
	lastIndex := rf.RaftLog.LastIndex()
	if lastIndex >= temp && temp > rf.RaftLog.committed {
		t, _ := rf.RaftLog.Term(temp)
		if t == rf.Term {
			//只能提交自己任期的日志
			old := rf.RaftLog.committed
			rf.RaftLog.committed = temp
			log.Debugf("S%d commit=%d->%d\n", rf.id, old, rf.RaftLog.committed)
			return true
		} else {
			tempTerm, _ := rf.RaftLog.Term(temp)
			log.Debugf("S%d can't update commit%d->%d,not my term%d!=%d\n", rf.id, rf.RaftLog.committed, temp, rf.Term, tempTerm)
		}
	}

	// apply
	return false
}
func (rf *Raft) applyMsg() {
	if rf.RaftLog.applied < rf.RaftLog.committed {
		for i := rf.RaftLog.applied; i <= rf.RaftLog.committed; i++ {
			rf.RaftLog.applied++
		}
	}
}

//-----------------------------------------------------------------------

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (rf *Raft) sendHeartbeatAll(m pb.Message) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != StateLeader {
		return
	}
	log.Debugf("S%d[%d] Leader,headBeat to ALL follower\n", m.From, m.Term)
	m.MsgType = pb.MessageType_MsgHeartbeat
	m.Commit = rf.RaftLog.committed
	m.Term = rf.Term
	m.From = rf.id
	peers := rf.Prs
	for peer := range peers {
		if peer == m.From {
			continue
		}
		m.To = peer
		rf.msgs = append(rf.msgs, m)
		log.Debugf("S%d[%d] heart to S%d", m.From, m.Term, m.To)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (rf *Raft) handleHeartbeat(req pb.Message) {
	reply := &pb.Message{
		MsgType:  pb.MessageType_MsgHeartbeatResponse,
		To:       req.From,
		From:     req.To,
		Term:     0,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	rf.mu.Lock()
	defer func() {
		rf.msgs = append(rf.msgs, *reply)
		rf.mu.Unlock()
	}()
	if req.Term < rf.Term {
		reply.Reject = true
		reply.Term = rf.Term
		log.Debugf("S%d S%d 's head beat,term<me.refused\n", rf.id, req.From)
		return
	}
	rf.initTime(true)
	log.Debugf("S%d[%d-%s] S%d[%d] 's head beat,revice ok\n", rf.id, rf.Term, rf.State, req.From, req.Term)
	if rf.State != StateFollower || rf.Term != req.Term || rf.Vote != req.From {
		rf.becomeFollower(req.Term, req.From)
	}
	reply.Term = rf.Term

	// update commit
	if req.GetCommit() > rf.RaftLog.committed {
		old := rf.RaftLog.committed
		rf.RaftLog.committed = uint64(int(math.Min(float64(req.GetCommit()), float64(req.GetIndex()+uint64(len(req.Entries))))))
		log.Debugf("S%d commit %d -> %d", rf.id, old, rf.RaftLog.committed)
	}
	reply.Commit = rf.RaftLog.committed
}

func (rf *Raft) heartBeatResponseHander(m pb.Message) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != StateLeader {
		return
	}
	if m.Term > rf.Term {
		rf.becomeFollower(m.Term, 0)
		return
	}
	if m.Commit < rf.RaftLog.committed {
		rf.sendAppend(m.GetFrom())
	}
}
