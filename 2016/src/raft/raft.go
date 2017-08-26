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
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leader                     int           // index to leader
	applyMsgCh                 chan ApplyMsg // apply message channel
	voteRequestReceived        bool          // whether received vote request or not
	heartbeatWithLeaderTimeout bool          // wether heart beat with leader is timeout or not

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term     int
	LeaderID int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term     int
	LeaderID int
	Voted    bool
}

type HeartBeatArgs struct {
	Term     int
	LeaderID int
}

type HeartBeatReply struct {
	Term              int
	LeaderID          int
	HeartBeatReceived bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.heartbeatWithLeaderTimeout {
		// If I havn't timeout for receive heart beat with last leader,
		// I will reject this vote
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = false
	} else if args.LeaderID == rf.me {
		// If this is a vote from myself, I will vote
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = true
	} else if args.Term <= rf.currentTerm {
		// If termid in this vote is less than or eaqul to my term id,
		// I will reject this vote also
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = false
	} else {
		// Else I will vote for the new leader
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = true
	}
}

// Handle heart beat from leader
func (rf *Raft) HandleHeartBeat(args HeartBeatArgs, reply *HeartBeatReply) {
	if args.Term == rf.currentTerm {
		if args.LeaderID == rf.leader {
			// Need to reset heart beat timeout timer and wait for next heart beat
		} else {
			// This is not a valid heart beat, do nothing
		}
	} else if args.Term < rf.currentTerm {
		// This is not a valid heart beat, do nothing
	} else {
		// This is heart beat from new leader, record new leader, reset heart beat
		// timeout timer and wait for next heart beat
		rf.leader = args.LeaderID
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HandleHeartBeat", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me != rf.leader {
		isLeader = false
	} else {
		index = len(rf.logs)
		term = rf.currentTerm

		// I'm leader now, add this log to my log list first
		newLogEntry := LogEntry{term, command}
		rf.logs = append(rf.logs, newLogEntry)

		// Initial a go routine to dispatch log to each peer
		go func(index int, command interface{}) {
			for _, peer := range rf.peers {
				peer.Call("", new(struct{}), new(struct{}))
			}

			// each time a new entry is committed to the log, each Raft peer
			// should send an ApplyMsg to the service (or tester).
			msg := ApplyMsg{Index: index, Command: command}
			rf.applyMsgCh <- msg
		}(index, command)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.applyMsgCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start a backgound routine to wait timeout
	go rf.checkTimeoutForVote()

	return rf
}

func (rf *Raft) checkTimeoutForVote() {
	time.Sleep(time.Millisecond * 1000)
	rf.mu.Lock()
	voteRequestReceived := rf.voteRequestReceived
	rf.mu.Unlock()

	// If I havn't received vote request after time out from any other peer,
	// I will initial a vote request
	if !voteRequestReceived {
		voteCount := 0
		for idx, _ := range rf.peers {
			rf.currentTerm++
			args := RequestVoteArgs{rf.currentTerm, rf.me}

			reply := new(RequestVoteReply)
			res := rf.sendRequestVote(idx, args, reply)
			if !res {
				continue
			}

			if reply.Voted {
				voteCount++
			}
		}

		// If I get majority of the vote, I will claim I am the new leader
		if voteCount > (len(rf.peers) / 2) {
			rf.StartHeartBeat()
		}
	}
}

func (rf *Raft) StartHeartBeat() {
	args := HeartBeatArgs{rf.currentTerm, rf.me}
	reply := new(HeartBeatReply)

	for idx, _ := range rf.peers {
		rf.sendHeartBeat(idx, args, reply)
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
}
