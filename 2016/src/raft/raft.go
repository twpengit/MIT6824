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
	"fmt"
	"math"
	"math/rand"
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
	heartbeatWithLeaderTimeout bool          // wether heart beat with leader is timeout or not

	// indicate there is a new leader election happen,no matter start vote or new
	// leader elected
	validLeaderHeartbeat chan bool
	// indicate I am elected as the new leader
	iAmNewLeader chan bool
	// indicate there is a new vote request when I am leader
	newVoteRequested chan bool
	// indicate I will step back as a follower now
	stepBack chan bool
	// indicate I have got dominate vote now
	getDominateVote chan bool
	// indicate I as a leader has got dominate agreement from other servers
	getDominateAgreementOnLog chan bool

	// current vote count for me in this term
	currentVoteCount int
	// current log agreement count for me in this term
	currentLogAgreementCount int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntity

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.me == rf.leader)

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
	Term         int
	LeaderID     int
	LastLogIndex int
	LastLogTerm  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("Receive vote request - I'm %d, my term is %d, got vote from %d, his term is %d\n",
		rf.me, rf.currentTerm, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject vote for candidator since it's log is not up to date
	if args.LastLogTerm < rf.currentTerm || args.LastLogIndex < len(rf.logs)-1 {
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = false
	}

	if args.Term > rf.currentTerm {
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = true

		rf.newVoteRequested <- true

		fmt.Printf("Vote - I'm %d, my term is %d, got vote from %d, his term is %d\n",
			rf.me, rf.currentTerm, args.LeaderID, args.Term)
		rf.currentTerm = args.Term
		rf.leader = args.LeaderID
	} else {
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = false

		fmt.Printf("Don't vote - I'm %d, my term is %d, got vote from %d, his term is %d\n",
			rf.me, rf.currentTerm, args.LeaderID, args.Term)
	}
}

// Append entries handler
func (rf *Raft) HandleAppendEntries(args LogEntry, reply *AppendLogEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Entries == nil {
		// This is a heart beat command
		if args.Term < rf.currentTerm {
			reply.LogEntryAppended = false
		} else {
			preTerm := rf.currentTerm
			rf.leader = args.LeaderId
			rf.currentTerm = args.Term

			reply.LogEntryAppended = true

			fmt.Printf("Handle heart beat in server %d, leader is %d, pre term is %d, current term is %d\n",
				rf.me, args.LeaderId, preTerm, rf.currentTerm)
			rf.validLeaderHeartbeat <- true
		}
	} else {
		// This is a normal append log entry command

		if len(rf.logs) == 0 {
			// this is the first log i have received
			rf.logs = append(rf.logs, args.Entries...)
			reply.LogEntryAppended = true

			// and set commit index according to the leader commit index
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs)-1)))
			}

			// each time a new entry is committed to the log, each Raft peer
			// should send an ApplyMsg to the service (or tester).
			for offset, entry := range args.Entries {
				msg := ApplyMsg{Index: offset, Command: entry.Command}
				rf.applyMsgCh <- msg
			}

			return
		}

		// if log term from leader is less then my latest term, i will reject this
		// agreement
		if args.PreLogTerm < rf.logs[len(rf.logs)-1].Term {
			reply.LogEntryAppended = false
			return
		}

		if len(rf.logs) < args.PreLogIndex {
			// if i do not have enough log as leader has, i will reject this
			// agreement
			reply.LogEntryAppended = false
			return
		} else if rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
			// if the term in the same index does not match that from leader, i
			// will reject this agreement and remove this log and logs follow that
			// log
			rf.logs = rf.logs[0 : len(rf.logs)-1]
			reply.LogEntryAppended = false
			return
		} else {
			// I agree on this log, append any new log into log list
			initialIndex := len(rf.logs)
			rf.logs = append(rf.logs, args.Entries...)
			reply.LogEntryAppended = true

			// and set commit index according to the leader commit index
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs)-1)))
			}

			// each time a new entry is committed to the log, each Raft peer
			// should send an ApplyMsg to the service (or tester).
			for offset, entry := range args.Entries {
				msg := ApplyMsg{Index: initialIndex + offset, Command: entry.Command}
				rf.applyMsgCh <- msg
			}
			return
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Voted && args.Term == rf.currentTerm {
		fmt.Printf("@vote from %d, current count %d, term is %d, preterm is %d\n", server, rf.currentVoteCount, rf.currentTerm, args.Term)
		rf.currentVoteCount++
		if rf.currentVoteCount > len(rf.peers)/2 {
			fmt.Printf("!!!%d is elected as new leader, current vote count is %d\n", rf.me, rf.currentVoteCount)
			rf.getDominateVote <- true
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args LogEntry, reply *AppendLogEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)

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

	if rf.me != rf.leader {
		// I'm not leader now, send to wrong server
		isLeader = false
		rf.mu.Unlock()

		return index, term, isLeader
	} else {
		index = len(rf.logs)
		term = rf.currentTerm

		preLogTerm := -1
		if len(rf.logs) != 0 {
			preLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		// I'm leader now, add this log to my log list first
		newLogEntry := LogEntry{term, rf.me, len(rf.logs) - 1,
			preLogTerm, []LogEntity{LogEntity{term, command}}, rf.commitIndex}
		rf.logs = append(rf.logs, LogEntity{term, command})

		// Initial go routines to dispatch log to each other peer
		rf.currentLogAgreementCount = 0

		for idx, _ := range rf.peers {
			if idx == rf.me {
				rf.currentLogAgreementCount++
				continue
			}

			go func(index int) {
				appendLogEntryReply := new(AppendLogEntryReply)

				for true {
					res := rf.sendAppendEntries(index, newLogEntry, appendLogEntryReply)
					if !res {
						continue
					} else if !appendLogEntryReply.LogEntryAppended {
						// peer don't agree on this log, try with previous logs
						term := newLogEntry.Term
						leaderId := newLogEntry.LeaderId
						preLogIndex := newLogEntry.PreLogIndex - 1
						preLogTerm := rf.logs[preLogIndex].Term
						entries := []LogEntity{rf.logs[preLogIndex]}
						entries = append(entries, newLogEntry.Entries...)
						leaderCommit := newLogEntry.LeaderCommit

						newLogEntry = LogEntry{term, leaderId, preLogIndex,
							preLogTerm, entries, leaderCommit}

						continue
					} else {
						rf.mu.Lock()

						if newLogEntry.LeaderCommit == rf.commitIndex {
							//  peer agree on this log
							rf.currentLogAgreementCount++

							if rf.currentLogAgreementCount > len(rf.peers)/2 {
								rf.getDominateAgreementOnLog <- true
							}
						} else {
							// Leader has already got dominate agreement on this
							// log, so ignore this agreement feedback
						}

						rf.mu.Unlock()

						break
					}
				}
			}(idx)
		}

		rf.mu.Unlock()

		// wait untile majority of server accept the log agreement, then we come
		// to the state of committment
		select {
		case <-rf.getDominateAgreementOnLog:
			rf.mu.Lock()

			// each time a new entry is committed to the log, each Raft peer
			// should send an ApplyMsg to the service (or tester).
			msg := ApplyMsg{Index: index, Command: command}
			rf.applyMsgCh <- msg

			// update last applied index first, this will be shared to other server
			// by leader heart beat or next log agreement process
			rf.commitIndex = len(rf.logs) - 1

			rf.mu.Unlock()

			return index, term, isLeader
		case <-rf.validLeaderHeartbeat:
			// I have got heart beat from new leader before I receive majority of
			// log agreement
			return -1, -1, false
		}
	}
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
	fmt.Printf("rf.me is %d\n", rf.me)

	// Your initialization code here.
	rf.applyMsgCh = applyCh
	rf.validLeaderHeartbeat = make(chan bool, 1)
	rf.iAmNewLeader = make(chan bool, 1)
	rf.newVoteRequested = make(chan bool, 1)
	rf.stepBack = make(chan bool, 1)
	rf.getDominateVote = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// I'm a candidator now, need to start the random timer to start election
	go rf.checkTimeoutForElection()

	// Be prepared that I am elected as the new leader also
	go rf.checkHeartBeat()

	return rf
}

func (rf *Raft) checkTimeoutForElection() {
	// I'm the follower(isCandidator = false) or candidator(isCandidator = true) now
	needRandomTimeout := true
	timeout := 0
	isElecting := false

	var timer *time.Timer = nil

	for {
		if needRandomTimeout {
			// if I am a candidator, I will use a random timeout to check
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			timeout = r.Intn(100)
		} else {
			// if I am a follower, I will use a fixed timeout to check
			timeout = 1000
		}

		if timer == nil {
			timer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
			fmt.Printf("Server %d check timeout for election as a %v, timeout is %d\n", rf.me, needRandomTimeout, timeout)
		} else {
			timer.Reset(time.Duration(timeout) * time.Millisecond)
			fmt.Printf("Server %d check timeout for election as a %v, reset timeout is %d\n", rf.me, needRandomTimeout, timeout)
		}

		select {
		case <-rf.validLeaderHeartbeat:
			// I have received a valid new leader heart beat before time out, so I
			// am a follower now, I will keep checking valid heart beat before timeout
			needRandomTimeout = false
			timer.Stop()
			fmt.Printf("server %d got validLeaderHeartbeat\n", rf.me)
		case <-rf.newVoteRequested:
			// I have received a valid vote request before time out, so I am a follower
			// now, I will keep checking valid heart beat before timeout
			needRandomTimeout = false
			timer.Stop()
			fmt.Printf("server %d got newVoteRequested\n", rf.me)
		case <-rf.getDominateVote:
			rf.mu.Lock()

			rf.leader = rf.me
			rf.iAmNewLeader <- true

			rf.mu.Unlock()

			// I'm leader now, wait untile I receive a step back notification
			<-rf.stepBack
			needRandomTimeout = true
		case <-timer.C:
			// havn't received heart beat from other peers, or havn't got
			// dominate votes, so I request vote from each of peer

			if isElecting {
				// If i am in electing and havn't got result yet, i will wait
				// another random time to start next round of election
				isElecting = false
				needRandomTimeout = true
				continue
			}

			rf.mu.Lock()
			//defer rf.mu.Unlock()

			// Reset validLeaderHeartbeat, newVoteRequested, getDominateVote
			// notification in case they happened at same time
			for i := 0; i < len(rf.validLeaderHeartbeat); i++ {
				<-rf.validLeaderHeartbeat
			}

			for i := 0; i < len(rf.newVoteRequested); i++ {
				<-rf.newVoteRequested
			}

			for i := 0; i < len(rf.getDominateVote); i++ {
				<-rf.getDominateVote
			}

			rf.leader = -1
			rf.currentTerm++
			rf.currentVoteCount = 0

			currentTerm := rf.currentTerm

			for idx, _ := range rf.peers {
				fmt.Printf("server %d request vote from %d\n", rf.me, idx)

				if idx == rf.me {
					// I always vote for myself
					rf.currentVoteCount++
					continue
				}

				args := RequestVoteArgs{currentTerm, rf.me, len(rf.logs) - 1,
					rf.logs[len(rf.logs)-1].Term}
				reply := new(RequestVoteReply)

				go rf.sendRequestVote(idx, args, reply)
			}

			isElecting = true
			needRandomTimeout = false

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) checkHeartBeat() {
LOOP:
	<-rf.iAmNewLeader

	// Send out heart beat now
	args := LogEntry{Term: rf.currentTerm, LeaderId: rf.me, Entries: nil}
	reply := new(AppendLogEntryReply)

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendAppendEntries(idx, args, reply)
	}

	for {
		select {
		case <-time.After(time.Duration(500) * time.Millisecond):
			// send heart beat to each peer
			args := LogEntry{Term: rf.currentTerm, LeaderId: rf.me, Entries: nil}
			reply := new(AppendLogEntryReply)

			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
				go rf.sendAppendEntries(idx, args, reply)
			}
		case <-rf.newVoteRequested:
			// received a new vote request
			rf.stepBack <- true
			goto LOOP
		case <-rf.validLeaderHeartbeat:
			// received a valid heart beat from new leader
			rf.stepBack <- true
			goto LOOP
		}
	}
}

type LogEntry struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntity
	LeaderCommit int
}

type LogEntity struct {
	Term    int
	Command interface{}
}

type AppendLogEntryReply struct {
	Term             int
	ServerID         int
	LogEntryAppended bool
}
