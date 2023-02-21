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

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	FOLLOWER         = 0
	CANDIDATE        = 1
	LEADER           = 2
	HEARTBEATTIMEOUT = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	Log         []*LogEntry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	VoteCount   int
	NodeState   int32
	HeartBeat   chan bool
	WinElection chan bool
	VoteChan    chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.CurrentTerm
	isleader = rf.NodeState == LEADER
	if isleader {
		fmt.Println("`````````````````````````````````````````````` term", rf.CurrentTerm)
	}
	fmt.Println("^^^^^^ leader state", rf.NodeState, "term", rf.CurrentTerm, "id", rf.me, "vote count", rf.VoteCount)

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
	LastLogIndex  int
	LastLogTerm   int
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
	LeaderTerm   int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.CandidateTerm < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.CurrentTerm
		fmt.Println("<<<<<<<<<<<<<< request fail at term", rf.CurrentTerm)
		return
	}
	reply.VoteGranted = false
	reply.CurrentTerm = rf.CurrentTerm
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		fmt.Println(">>>>>>>>>>>>>>>>>> get vote")
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		// send vote
		rf.sendVote()
		fmt.Printf("****** %d vote to %d in term %d\n", rf.me, args.CandidateID, rf.CurrentTerm)
	} else if args.CandidateTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.CandidateTerm
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		// fmt.Printf("<<<<<<<<< %d become %d's follower in term %d %d >>>>>>>>\n", rf.me, args.CandidateID, rf.CurrentTerm, args.CandidateTerm)
		rf.UpdateState(FOLLOWER)
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm < rf.CurrentTerm {
		reply.Success = false
		args.LeaderTerm = rf.CurrentTerm
		return
	}
	if args.LeaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.LeaderTerm
		rf.UpdateState(FOLLOWER)
	}
	reply.CurrentTerm = rf.CurrentTerm
	// rf.CurrentTerm = reply.CurrentTerm
	rf.sendHeartbeat()
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.CurrentTerm++
	rf.mu.Unlock()
	args := RequestVoteArgs{
		CandidateTerm: rf.CurrentTerm,
		CandidateID:   rf.me,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			fmt.Printf("#### term %d, machine %d send request to machine %d term %d\n", rf.CurrentTerm, args.CandidateID, server, args.CandidateTerm)
			if rf.NodeState == CANDIDATE && rf.sendRequestVote(server, &args, &reply) {
				if rf.NodeState != CANDIDATE && args.CandidateTerm != rf.CurrentTerm {
					return
				}
				rf.mu.Lock()
				if reply.VoteGranted {
					// rf.sendVote()
					rf.VoteCount++
					if rf.VoteCount > len(rf.peers)/2 {
						fmt.Println("@@@ win election")
						rf.sendWinElection()
						// return
					}
				} else {
					if reply.CurrentTerm > rf.CurrentTerm {
						fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
						rf.CurrentTerm = reply.CurrentTerm
						rf.UpdateState(FOLLOWER)
					}
				}
				rf.mu.Unlock()
			} else {
				fmt.Printf("^^^ machine %d call machine %d fail\n", rf.me, server)
			}

		}(i)
	}
}

func (rf *Raft) BoardCastAppendEntries() {
	args := AppendEntriesArgs{
		LeaderID:   rf.me,
		LeaderTerm: rf.CurrentTerm,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			if rf.NodeState != LEADER {
				return
			}
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if rf.NodeState != LEADER || rf.CurrentTerm > reply.CurrentTerm || rf.CurrentTerm != args.LeaderTerm {
					return
				}
				if rf.CurrentTerm < reply.CurrentTerm {
					rf.CurrentTerm = reply.CurrentTerm
					rf.UpdateState(FOLLOWER)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) UpdateState(state int32) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.NodeState == state {
		return
	}
	// old_state := rf.NodeState
	switch state {
	case FOLLOWER:
		rf.NodeState = FOLLOWER
		// rf.VoteCount = 0
	case CANDIDATE:
		rf.NodeState = CANDIDATE
	case LEADER:
		rf.NodeState = LEADER
	default:
		fmt.Println("unknown state: ", state)
	}
	// fmt.Printf(">>> term %d update state from %d to %d from %d\n", rf.CurrentTerm, old_state, rf.NodeState, rf.me)
}

// func (rf *Raft) Broadcast() {}

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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	go func() {
		rf.HeartBeat <- true
	}()
}

func (rf *Raft) sendWinElection() {
	go func() {
		rf.WinElection <- true
	}()
}

func (rf *Raft) sendVote() {
	go func() {
		rf.VoteChan <- true
	}()
}

func RandElectionTime() time.Duration {
	rand.Seed(time.Now().UnixNano() / 1e6)
	timeNum := rand.Intn(150) + 200
	// fmt.Println( timeNum)
	return time.Millisecond * time.Duration(timeNum)
}

func RandBoardcastTime() time.Duration {
	rand.Seed(time.Now().UnixNano() / 1e6)
	return time.Millisecond * time.Duration(rand.Intn(100)+50)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 先监听心跳再监听选举时间
		// switch rf.NodeState {
		switch atomic.LoadInt32(&rf.NodeState) {
		case FOLLOWER:
			select {
			case <-rf.HeartBeat:
				// fmt.Println("------ follower get heartbeat")
			case <-time.After(RandElectionTime()):
				rf.mu.Lock()
				rf.UpdateState(CANDIDATE)
				// fmt.Println(atomic.LoadInt32(&rf.NodeState))
				rf.mu.Unlock()
			}
		case CANDIDATE:
			select {
			case <-rf.HeartBeat:
				// fmt.Println("----- candidate get heartbeat")
				rf.mu.Lock()
				rf.UpdateState(FOLLOWER)
				rf.mu.Unlock()
			case <-time.After(RandElectionTime()):
				rf.StartElection()
			case <-rf.WinElection:
				rf.mu.Lock()
				rf.UpdateState(LEADER)
				// fmt.Println("==== term", rf.CurrentTerm, rf.me, "become leader")
				rf.mu.Unlock()
				// case <-time.After(300 * time.Millisecond):
				// 	fmt.Println("*** become follower again")
				// 	rf.mu.Lock()
				// 	rf.UpdateState(FOLLOWER)
				// 	rf.mu.Unlock()

			}

		case LEADER:
			rf.mu.Lock()
			rf.BoardCastAppendEntries()
			rf.mu.Unlock()
			time.Sleep(50 * time.Microsecond)

		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.VoteCount = 0
	rf.NodeState = FOLLOWER
	rf.HeartBeat = make(chan bool)
	rf.WinElection = make(chan bool)
	rf.VoteChan = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
