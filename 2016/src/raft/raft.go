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

	applyCh           chan ApplyMsg
	leaderHeartBeatCh chan bool
	voteGrantedCh     chan bool
	electedAsLeaderCh chan bool
	voteCount         int
	currentState      serverState

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []logEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders(Reinitialized after election):
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
	isleader = (rf.currentState == leader)

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
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("RequestVote - I'm %d, my term is %d, candidate is %d, candidate term is %d\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)

	// Set default reply first
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// Keep default reply
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogTerm := 0
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].term
			}

			if args.LastLogTerm < lastLogTerm {
				// Keep default reply
			} else if args.LastLogIndex < len(rf.log) {
				// Keep default reply
			} else {
				rf.currentTerm = args.Term
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true

			CLEAN_A:
				for true {
					// Clear messages in channel
					select {
					case <-rf.voteGrantedCh:
					default:
						break CLEAN_A
					}
				}
				rf.voteGrantedCh <- true

				fmt.Printf("Vote granted- I'm %d, my term is %d, candidate is %d, candidate term is %d\n",
					rf.me, rf.currentTerm, args.CandidateId, args.Term)
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	} else {
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].term
		}

		if args.LastLogTerm < lastLogTerm {
			// Keep default reply
		} else if args.LastLogIndex < len(rf.log) {
			// Keep default reply
		} else {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

		CLEAN_B:
			for true {
				// Clear messages in channel
				select {
				case <-rf.voteGrantedCh:
				default:
					break CLEAN_B
				}
			}
			rf.voteGrantedCh <- true

			fmt.Printf("Vote granted- I'm %d, my term is %d, candidate is %d, candidate term is %d\n",
				rf.me, rf.currentTerm, args.CandidateId, args.Term)
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Entries == nil {
		// This is a heart beat from leader
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentState = follower
			rf.votedFor = args.LeaderId

			reply.Success = true
			reply.Term = args.Term

		CLEAN:
			for true {
				// Clear messages in channel
				select {
				case <-rf.leaderHeartBeatCh:
				default:
					break CLEAN
				}
			}
			rf.leaderHeartBeatCh <- true
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

func (rf *Raft) Execute() {
	for true {
		switch rf.currentState {
		case follower:
			// Wait for the heart beat from leader or grant vote for candidate
			timer := time.NewTimer(followerTimeoutInterval * time.Millisecond)

			select {
			case <-timer.C:
				fmt.Printf("Execute - server:%d, current state:%d wait timeout\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = candidate
			case <-rf.leaderHeartBeatCh:
				fmt.Printf("Execute - server:%d, current state:%d server HB\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			case <-rf.voteGrantedCh:
				fmt.Printf("Execute - server:%d, current state:%d vote granted\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			}
		case candidate:
			// Wait for a random timeout to start election for myself
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			timeout := r.Intn(int(candidateRandomTimeoutInterval))

			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)

			select {
			case <-timer.C:
				fmt.Printf("Execute - server:%d, current state:%d wait %d timeout\n",
					rf.me, rf.currentState, timeout)
				timer.Stop()
				rf.mu.Lock()
				rf.voteCount = 0
				rf.votedFor = rf.me
				rf.currentTerm++

				for idx, _ := range rf.peers {
					if idx == rf.me {
						rf.voteCount++
					} else {
						lastLogTerm := 0
						if len(rf.log) > 0 {
							lastLogTerm = rf.log[len(rf.log)-1].term
						}
						voteArgs := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log),
							lastLogTerm}
						voteReply := new(RequestVoteReply)

						go func(args RequestVoteArgs, reply *RequestVoteReply) {
						RETRY:
							for true {
								res := rf.sendRequestVote(idx, args, reply)
								if res {
									rf.mu.Lock()
									fmt.Printf("1\n")
									if reply.Term == rf.currentTerm &&
										reply.VoteGranted &&
										rf.currentState == candidate {
										rf.voteCount++
										if (rf.voteCount > len(rf.peers)/2) &&
											((rf.voteCount - 1) <= len(rf.peers)/2) {
											fmt.Printf("2\n")
										CLEAN:
											for true {
												// Clear messages in channel
												select {
												case <-rf.electedAsLeaderCh:
												default:
													break CLEAN
												}
											}
											rf.electedAsLeaderCh <- true
										}
									}

									rf.mu.Unlock()
									break RETRY
								}
							}

						}(voteArgs, voteReply)
					}
				}

				rf.mu.Unlock()
			case <-rf.voteGrantedCh:
				fmt.Printf("Execute - server:%d, current state:%d vote granted\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			case <-rf.leaderHeartBeatCh:
				fmt.Printf("Execute - server:%d, current state:%d server HB\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			case <-rf.electedAsLeaderCh:
				fmt.Printf("Execute - server:%d, current state:%d elected as leader\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = leader
			}
		case leader:
			timer := time.NewTimer(time.Duration(leaderTimeoutInterval) * time.Millisecond)

			select {
			case <-timer.C:
				fmt.Printf("Execute - server:%d, current state:%d send HB\n",
					rf.me, rf.currentState)
				timer.Stop()
				for idx, _ := range rf.peers {
					if idx == rf.me {
						// Skip sending heart beat to myself
					} else {
						lastLogTerm := 0
						if len(rf.log) > 0 {
							lastLogTerm = rf.log[len(rf.log)-1].term
						}
						heartBeatArgs := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log),
							lastLogTerm, nil, rf.commitIndex}
						heartBeatReply := new(AppendEntriesReply)

						go func(args AppendEntriesArgs, reply *AppendEntriesReply) {
							rf.sendAppendEntries(idx, args, reply)
						}(heartBeatArgs, heartBeatReply)
					}
				}
			case <-rf.leaderHeartBeatCh:
				fmt.Printf("Execute - server:%d, current state:%d server HB\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			case <-rf.voteGrantedCh:
				fmt.Printf("Execute - server:%d, current state:%d vote granted\n",
					rf.me, rf.currentState)
				timer.Stop()
				rf.currentState = follower
			}
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
	fmt.Printf("Me:%d\n", me)
	rf.me = me

	// Your initialization code here.
	rf.applyCh = applyCh
	rf.currentState = follower

	rf.leaderHeartBeatCh = make(chan bool, 1)
	rf.voteGrantedCh = make(chan bool, 1)
	rf.electedAsLeaderCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Execute()

	return rf
}

type logEntry struct {
	command interface{}
	term    int
}

type serverState int

const (
	follower serverState = iota
	candidate
	leader
)

const (
	followerTimeoutInterval        time.Duration = 1000
	candidateRandomTimeoutInterval time.Duration = 750
	leaderTimeoutInterval          time.Duration = 500
)
