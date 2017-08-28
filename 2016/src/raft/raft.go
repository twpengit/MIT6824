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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("Receive vote request - I'm %d, my term is %d, got vote from %d, his term is %d\n",
		rf.me, rf.currentTerm, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		reply.LeaderID = args.LeaderID
		reply.Term = args.Term
		reply.Voted = true

		rf.newVoteRequested <- true

		fmt.Printf("Vote - I'm %d, my term is %d, got vote from %d, his term is %d\n",
			rf.me, rf.currentTerm, args.LeaderID, args.Term)
		rf.currentTerm = args.Term
	} else {
		fmt.Println("1")
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

	if args.Command == nil {
		// This is a heart beat command
		if args.Term < rf.currentTerm {
			reply.LogEntryAppended = false
		} else {
			rf.leader = args.ServerID
			rf.currentTerm = args.Term

			reply.LogEntryAppended = true

			go func() {
				fmt.Printf("Handle heart beat in server %d\n", rf.me)
				rf.validLeaderHeartbeat <- true
			}()
		}
	} else {
		// This is a normal append log entry command
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

func (rf *Raft) sendHeartBeat(server int, args LogEntry, reply *AppendLogEntryReply) bool {
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

	if rf.me != rf.leader {
		isLeader = false
	} else {
		index = len(rf.logs)
		term = rf.currentTerm

		// I'm leader now, add this log to my log list first
		newLogEntry := LogEntry{term, rf.me, command}
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
	fmt.Printf("rf.me is %d\n", rf.me)

	// Your initialization code here.
	rf.applyMsgCh = applyCh
	rf.validLeaderHeartbeat = make(chan bool, 1)
	rf.iAmNewLeader = make(chan bool, 1)
	rf.newVoteRequested = make(chan bool, 1)
	rf.stepBack = make(chan bool, 1)

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
	isCandidator := true
	timeout := 0
	voteCount := 0

	var timer *time.Timer = nil

	for {
		if isCandidator {
			// if I am a candidator, I will use a random timeout to check
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			timeout = r.Intn(100)
			voteCount = 0
		} else {
			// if I am a follower, I will use a fixed timeout to check
			timeout = 2000
		}

		if timer == nil {
			timer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
			fmt.Printf("Server %d check timeout for election as a %v, timeout is %d\n", rf.me, isCandidator, timeout)
		} else {
			timer.Reset(time.Duration(timeout) * time.Millisecond)
			fmt.Printf("Server %d check timeout for election as a %v, reset timeout is %d\n", rf.me, isCandidator, timeout)
		}

		select {
		case <-rf.validLeaderHeartbeat:
			// I have received a valid new leader heart beat before time out, so I
			// am a follower now, I will keep checking valid heart beat before timeout
			isCandidator = false
			timer.Stop()
			fmt.Printf("server %d got validLeaderHeartbeat\n", rf.me)
		case <-rf.newVoteRequested:
			// I have received a valid vote request before time out, so I am a follower
			// now, I will keep checking valid heart beat before timeout
			isCandidator = false
			timer.Stop()
			fmt.Printf("server %d got newVoteRequested\n", rf.me)
		case <-timer.C:
			// havn't received heart beat from other peers, so I request vote from
			// each of peer

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Reset validLeaderHeartbeat or newVoteRequested notification in case
			// they happened at same time
			for i := 0; i < len(rf.validLeaderHeartbeat); i++ {
				<-rf.validLeaderHeartbeat
			}

			for i := 0; i < len(rf.newVoteRequested); i++ {
				<-rf.newVoteRequested
			}

			isCandidator = true
			rf.currentTerm++

			for idx, _ := range rf.peers {
				fmt.Printf("server %d request vote from %d\n", rf.me, idx)

				if idx == rf.me {
					// I always vote for myself
					voteCount++
					continue
				}

				args := RequestVoteArgs{rf.currentTerm, rf.me}
				reply := new(RequestVoteReply)
				res := rf.sendRequestVote(idx, args, reply)

				if !res {
					fmt.Printf("Server %d request vote failed\n", rf.me)
					continue
				} else if reply.Voted {
					voteCount++
					fmt.Printf("Server %d got vote %d\n", rf.me, voteCount)
				} else {
					fmt.Printf("Server %d got vote rejected %d\n", rf.me, voteCount)
				}
			}

			if voteCount > len(rf.peers)/2 {
				fmt.Println("HeartBeat1")
				// I received majority of the votes, I am elected as the new leader
				rf.iAmNewLeader <- true
				// I'm leader now, wait untile I receive a step back notification
				<-rf.stepBack
				isCandidator = false
			}
		}
	}
}

func (rf *Raft) checkHeartBeat() {
LOOP:
	<-rf.iAmNewLeader

	// Send out heart beat now
	fmt.Println("HeartBeat2")
	args := LogEntry{rf.currentTerm, rf.me, nil}
	reply := new(AppendLogEntryReply)

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.sendHeartBeat(idx, args, reply)
	}

	for {
		select {
		case <-time.After(time.Duration(500) * time.Millisecond):
			// send heart beat to each peer
			args := LogEntry{rf.currentTerm, rf.me, nil}
			reply := new(AppendLogEntryReply)

			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
				rf.sendHeartBeat(idx, args, reply)
			}
		case <-rf.newVoteRequested:
			// received a new vote request
			rf.stepBack <- true
			goto LOOP
		}
	}
}

type LogEntry struct {
	Term     int
	ServerID int
	Command  interface{}
}

type AppendLogEntryReply struct {
	Term             int
	ServerID         int
	LogEntryAppended bool
}
