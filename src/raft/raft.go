package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State uint32

const (
	follower State = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	state             State
	curTerm           int
	heartBeat         time.Duration
	timeOut           time.Time
	log               []LogEntry
	votedFor          int
	majorityServerNum int
	cond              sync.Cond
	applyCh           chan ApplyMsg

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.curTerm
	// Your code here (2A).
	return term, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.curTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Me           int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	LastIndex   int
	LastLogTerm int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Server:%d In Term%d Recive VoteRequest", rf.me, rf.curTerm)
	reply.Term = rf.curTerm
	reply.VoteGranted = false
	if args.Term < rf.curTerm {
		return
	}
	rf.syncTerm(args.Term)
	// up-to-date
	isUpToDate := args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log))
	if (rf.votedFor == -1 || rf.votedFor == args.Me) && isUpToDate {
		rf.votedFor = args.Me
		rf.persist()
		// DPrintf("Server:%d In Term:%d GrantedVote For:%d", rf.me, rf.curTerm, args.Me)
		reply.VoteGranted = true
		rf.resetTimeOut()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm
	reply.Success = false
	if args.Term < rf.curTerm {
		return
	}
	rf.votedFor = args.LeaderId
	rf.syncTerm(args.Term)
	rf.resetTimeOut()
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.LastIndex = len(rf.log) - 1
		reply.LastLogTerm = rf.log[len(rf.log)-1].Term
		return
	} else {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("Del Server:%d, Before:%v", rf.me, rf.log)
			rf.log = append(rf.log[:args.PrevLogIndex], rf.log[args.PrevLogIndex+1:]...)
			rf.persist()
			reply.LastIndex = len(rf.log) - 1
			reply.LastLogTerm = rf.log[len(rf.log)-1].Term
			DPrintf("Del Server:%d After:%v", rf.me, rf.log)
			return
		}
	}

	if len(args.Log) != 0 {
		DPrintf("args:%+v", args)
		DPrintf("Append Server:%d, Before:%v", rf.me, rf.log)
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Log...)
		rf.persist()
		DPrintf("Append Server:%d After:%v", rf.me, rf.log)
	} else {
		// DPrintf("server:%d In Term:%d Recive HeartBeat Leader Term:%d", rf.me, rf.curTerm, args.Term)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	rf.cond.Broadcast()
	reply.LastLogTerm = rf.log[len(rf.log)-1].Term
	reply.LastIndex = len(rf.log) - 1
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.curTerm
	isLeader := rf.state == leader
	if !isLeader {
		return -1, term, isLeader
	}

	entry := LogEntry{
		Term:    rf.curTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	// go rf.broadcast()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.curTerm = 0
	rf.heartBeat = 60 * time.Millisecond
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	// index start with 1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{-1, 0}
	rf.majorityServerNum = len(rf.peers) / 2
	rf.cond = *sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.resetTimeOut()

	// Your initialization code here (2A, 2B, 2C).
	go rf.ticker()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.loopForApply()

	return rf
}

// rule All Servers 1
func (rf *Raft) loopForApply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("Server:%d ApplyMsg:%v CommiteIdx:%d", rf.me, applyMsg, rf.commitIndex)
			// DPrintf("Server:%d Commited Log:%v", rf.me, rf.log[:rf.lastApplied+1])
			rf.applyCh <- applyMsg
		} else {
			rf.cond.Wait()
		}
	}
}

// 以心跳时间为定时单位，如果是Leader发送心跳，同时检查是否超时，所以超时不能
// 立即检查到，会有心跳时间的误差
func (rf *Raft) ticker() {
	for {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if time.Now().After(rf.timeOut) {
			rf.state = candidate
			rf.toElection()
		}
		if rf.state == leader {
			// DPrintf("Server:%d In Term:%d Send HeartBeat", rf.me, rf.curTerm)
			go rf.broadcast()
		}
		rf.mu.Unlock()
	}
}

// rule All Servers 2
func (rf *Raft) syncTerm(term int) bool {
	if term > rf.curTerm {
		DPrintf("Server:%d Term:%d New Term:%d", rf.me, rf.curTerm, term)
		rf.state = follower
		rf.curTerm = term
		rf.votedFor = -1
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) broadcast() {
	rf.resetTimeOut()
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// send之后会更新nextIndex，这里需要使用更新后的nextIndex
		rf.mu.Lock()
		nextIndex := rf.nextIndex[idx]
		if rf.nextIndex[idx] > len(rf.log) {
			nextIndex = len(rf.log)
		}
		args := AppendEntriesArgs{
			Term:         rf.curTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log[nextIndex-1].Term,
			Log:          make([]LogEntry, len(rf.log)-nextIndex),
		}
		// 有新log 否则是heartbeat
		if len(rf.log) > rf.nextIndex[idx] {
			copy(args.Log, rf.log[rf.nextIndex[idx]:])
		}
		rf.mu.Unlock()
		go rf.send(idx, &args)
	}
}

func (rf *Raft) send(serverId int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 当前任期过小失败
	if rf.syncTerm(reply.Term) {
		return
	}
	// prevLog不匹配失败
	rf.nextIndex[serverId] = reply.LastIndex + 1
	DPrintf("Server%d NextIndex:%d", serverId, reply.LastIndex+1)
	// if !reply.Success {
	// 	rf.nextIndex[serverId]--
	// }

	if reply.Success {
		// update matchIndex
		rf.matchIndex[serverId] = reply.LastIndex

		rf.setCommitId()
	}
}

func (rf *Raft) setCommitId() {
	counter := 1
	minimum := 0x7fffffff
	for _, val := range rf.matchIndex {
		if val > rf.commitIndex {
			counter++
			minimum = min(minimum, val)
		}
	}
	minimum = min(minimum, len(rf.log)-1)
	if counter > rf.majorityServerNum && rf.log[minimum].Term == rf.curTerm {
		rf.commitIndex = minimum
	}
	rf.apply()
}

func (rf *Raft) apply() {
	// 唤醒LoopForApply
	rf.cond.Broadcast()
}

func (rf *Raft) resetTimeOut() {
	t := time.Now()
	rf.timeOut = t.Add(time.Duration(140+rand.Intn(160)) * time.Millisecond)
}

func (rf *Raft) toElection() {
	rf.curTerm++
	voteCounter := 1
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTimeOut()
	// DPrintf("Server:%d In Term:%d Send VoteRequest:", rf.me, rf.curTerm)
	args := RequestVoteArgs{
		Term:         rf.curTerm,
		Me:           rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.collectVote(idx, &args, &reply, &voteCounter)
	}

}

func (rf *Raft) collectVote(serverId int, args *RequestVoteArgs, reply *RequestVoteReply, voteCounter *int) {
	ok := rf.sendRequestVote(serverId, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.syncTerm(reply.Term)
	if rf.state != candidate || !reply.VoteGranted {
		return
	}
	*voteCounter++
	// DPrintf("Server:%d In Term:%d VoteNumber:%d", rf.me, rf.curTerm, *voteCounter)

	if *voteCounter > rf.majorityServerNum {
		DPrintf("Server:%d In Term:%d Be Leader", rf.me, rf.curTerm)
		rf.state = leader
		// reinitialized after election
		for idx, _ := range rf.peers {
			rf.nextIndex[idx] = len(rf.log)
			rf.matchIndex[idx] = 0
		}
		DPrintf("Leader:%d Log %v", rf.me, rf.log)
		go rf.broadcast()
	}
}

func (rf *Raft) CommitedLog() {
	DPrintf("Server:%d %v commiteIndex:%d", rf.me, rf.log, rf.commitIndex)
}
