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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	state           int32               // 0 follower 1 candidate 2 leader
	currentTerm     int
	votedFor        int
	votedCount      int
	heartBeatChan   chan bool
	voteRequestChan chan bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

const (
	FOLLOWER  int32 = 0
	CANDIDATE int32 = 1
	LEADER    int32 = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	isleader = rf.state == LEADER
	term = rf.currentTerm
	if isleader {
		fmt.Println(">>>>>>> node", rf.me, "term", rf.currentTerm, "become leader")
	}
	fmt.Println("--------- node", rf.me, "state", rf.currentTerm, "state", rf.state, "ticket", rf.votedCount)
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateID   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderID   int
}

type AppendEntriesReply struct {
	CurrentTerm int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("rf:", rf.me, " state:", rf.state, "term:", rf.currentTerm)
	reply.VoteGranted = false
	reply.CurrentTerm = rf.currentTerm
	if rf.currentTerm >= args.CandidateTerm {
		fmt.Println("candidate ", args.CandidateID, "candidateTerm", args.CandidateTerm, "request vote from", rf.me, "term", rf.currentTerm, "failed")
		return
	}
	rf.voteRequestChan <- true
	// rf.state = FOLLOWER
	rf.currentTerm = args.CandidateTerm
	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	// reply.CurrentTerm = rf.currentTerm
	// fmt.Println("candidate ", args.CandidateID, " request vote from", rf.me, "success", reply.VoteGranted)
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	if rf.state != CANDIDATE || rf.votedFor != rf.me {
		rf.mu.Unlock()
		return
	}
	rf.votedCount = 1
	rf.currentTerm++
	fmt.Println("----  node ", rf.me, "term", rf.currentTerm, "vote for", rf.votedFor, "start election")
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateID:   rf.me,
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	reply.CurrentTerm = rf.currentTerm
	if rf.currentTerm > args.LeaderTerm {
		// reply.CurrentTerm = rf.currentTerm
		return
	}
	rf.currentTerm = args.LeaderTerm
	reply.CurrentTerm = rf.currentTerm
	rf.heartBeatChan <- true
}

func (rf *Raft) SendHeartBeat() {
	args := AppendEntriesArgs{
		LeaderTerm: rf.currentTerm,
		LeaderID:   rf.me,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Println("--- node", rf.me, "term", rf.currentTerm, "get ticket from", server, reply.VoteGranted)
	// fmt.Println("----- node",rf.me, "rf.term",rf.currentTerm, "reply.term",reply.CurrentTerm)
	if reply.CurrentTerm > rf.me {
		rf.currentTerm = reply.CurrentTerm
		rf.state = FOLLOWER
		rf.votedCount = 0
		return ok
	} else if rf.currentTerm == reply.CurrentTerm {
		return ok
	}
	if reply.VoteGranted {
		rf.votedCount++
	}
	if rf.votedCount > len(rf.peers)/2 {
		rf.state = LEADER
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.CurrentTerm {
		rf.currentTerm = reply.CurrentTerm
		rf.state = FOLLOWER
		return ok
	}
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func ElectionTimer() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func HeartbeatTimer() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(100)+150) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			rf.mu.Lock()
			rf.votedCount = 0
			rf.mu.Unlock()
			select {
			case <-rf.heartBeatChan:
			case <-rf.voteRequestChan:
			case <-time.After(ElectionTimer()):
				rf.mu.Lock()
				rf.state = CANDIDATE
				// rf.votedFor = rf.me
				rf.mu.Unlock()
				rf.StartElection()
			}

			// time.Sleep(ElectionTimer())
		case CANDIDATE:
			select {
			case <-rf.voteRequestChan:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-rf.heartBeatChan:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-time.After(ElectionTimer()):
				rf.StartElection()
			}
			// time.Sleep(ElectionTimer())
		case LEADER:
			select {
			case <-time.After(HeartbeatTimer()):
				rf.SendHeartBeat()
			case <-rf.voteRequestChan:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-rf.heartBeatChan:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()

			}
			// time.Sleep(HeartbeatTimer())
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// time.Sleep(time.Millisecond * 500)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		state:           FOLLOWER,
		currentTerm:     0,
		votedFor:        me,
		votedCount:      0,
		heartBeatChan:   make(chan bool),
		voteRequestChan: make(chan bool),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
