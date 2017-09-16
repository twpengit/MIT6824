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

	applyCh           chan ApplyMsg
	leaderHeartBeatCh chan bool
	voteGrantedCh     chan bool
	electedAsLeaderCh chan bool
	voteCount         int
	currentState      serverState

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

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
				lastLogTerm = rf.log[len(rf.log)-1].Term
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
			lastLogTerm = rf.log[len(rf.log)-1].Term
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
	Entries      []LogEntry
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
		fmt.Printf("Heart beat from %d, leader term is %d, I'm %d, my term is %d\n",
			args.LeaderId, args.Term, rf.me, rf.currentTerm)
		// This is a heart beat from leader
		//		if args.Term >= rf.currentTerm {
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

		// leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit),
				float64(len(rf.log))))
		}
		//		} else {
		//			reply.Success = false
		//			reply.Term = rf.currentTerm
		//		}
	} else {
		// Set default reply
		reply.Success = false
		reply.Term = rf.currentTerm

		// Reply false if term < currentTerm (§5.1)
		if rf.currentTerm > args.Term {
			// Keep default reply
			return
		}

		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if len(rf.log) < args.PreLogIndex {
			// Keep default reply
			return
		}
		if len(rf.log) > 0 && args.PreLogIndex > 0 &&
			rf.log[args.PreLogIndex-1].Term != args.PreLogTerm {
			// Keep default reply, remove unmatched logs
			rf.log = rf.log[0 : args.PreLogIndex-1]
			return
		}

		fmt.Printf("*applyLogs, server %d log before append...\n", rf.me)
		for idx, command := range rf.log {
			fmt.Printf("server:%d, idx:%d, command:%d\n", rf.me, idx, command.Command.(int))
		}

		// Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)

		fmt.Printf("*applyLogs, server %d log after append...\n", rf.me)
		for idx, command := range rf.log {
			fmt.Printf("server:%d, idx:%d, command:%d\n", rf.me, idx, command.Command.(int))
		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit),
				float64(len(rf.log))))
		}

		reply.Success = true
		reply.Term = rf.currentTerm
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
	fmt.Printf("*applyLogs Start, server %d, commitIndex %d, lastApplied %d, command %d\n",
		rf.me, rf.commitIndex, rf.lastApplied, command.(int))
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()

	if rf.currentState != leader {
		fmt.Printf("*applyLogs Start false, server %d, commitIndex %d, lastApplied %d, command %d\n",
			rf.me, rf.commitIndex, rf.lastApplied, command.(int))
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	fmt.Printf("*applyLogs, leader %d log before append...\n", rf.me)
	for idx, command := range rf.log {
		fmt.Printf("server:%d, idx:%d, command:%d\n", rf.me, idx, command.Command.(int))
	}

	// I'm the leader, append log first
	logEntry := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, logEntry)

	fmt.Printf("*applyLogs, leader %d log after append...\n", rf.me)
	for idx, command := range rf.log {
		fmt.Printf("server:%d, idx:%d, command:%d\n", rf.me, idx, command.Command.(int))
	}

	// Start distributing logs to other peers
	replyChn := make(chan bool, 1)
	go rf.distributeLogs(replyChn)

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true

	rf.mu.Unlock()

	// Wait for reply
	<-replyChn

	// Wait for log to be applied
RETURN:
	for true {
		timer := time.NewTimer(applyLogTimeoutInterval * time.Millisecond)

		select {
		case <-timer.C:
			// If I am leader, I will try to commit and apply logs
			timer.Stop()

			rf.mu.Lock()
			if rf.lastApplied >= index {
				rf.mu.Unlock()
				break RETURN
			}
			rf.mu.Unlock()
		}
	}

	return index, term, isLeader
}

// Distribute logs to each other peer
func (rf *Raft) distributeLogs(replyChannel chan bool) {
	// Reset commit count for this log distribution
	commitCount := 0
	var commitCountMutex sync.Mutex
	dominateCommitChan := make(chan bool, len(rf.peers))

	for idx, _ := range rf.peers {
		if idx == rf.me {
			// Skip sending log to myself
			commitCountMutex.Lock()
			commitCount++

			fmt.Printf("Commit count for myself is %d\n", commitCount)

			if commitCount > len(rf.peers)/2 &&
				commitCount-1 <= len(rf.peers)/2 {
				dominateCommitChan <- true
			}
			commitCountMutex.Unlock()

			continue
		} else {
			preLogIndex := 0
			if rf.nextIndex[idx] > 1 {
				preLogIndex = rf.nextIndex[idx] - 1
			}
			preLogTerm := 0
			if preLogIndex > 0 {
				preLogTerm = rf.log[preLogIndex-1].Term
			}
			appendLogArgs := AppendEntriesArgs{rf.currentTerm, rf.me, preLogIndex,
				preLogTerm, rf.log[rf.nextIndex[idx]-1:], rf.commitIndex}
			appendLogReply := new(AppendEntriesReply)

			go func(index int, args AppendEntriesArgs, reply *AppendEntriesReply) {
				//				for true {
				res := rf.sendAppendEntries(index, args, reply)
				if res {
					if reply.Success {
						commitCountMutex.Lock()
						// Update next index and match index for this peer
						rf.mu.Lock()
						rf.nextIndex[index] = args.PreLogIndex + len(args.Entries) + 1
						rf.matchIndex[index] = args.PreLogIndex + len(args.Entries)
						rf.mu.Unlock()

						// Increase commit count
						commitCount++

						fmt.Printf("Commit count for server %d is %d\n", index, commitCount)

						if commitCount > len(rf.peers)/2 &&
							commitCount-1 <= len(rf.peers)/2 {
							dominateCommitChan <- true
						}
						commitCountMutex.Unlock()

						//						break
					} else {
						// Decrease next index and retry next time
						rf.mu.Lock()
						if rf.nextIndex[idx] == 1 {
							//							rf.mu.Unlock()
							//							break
						} else {
							rf.nextIndex[idx] = rf.nextIndex[idx] - 1
						}
						rf.mu.Unlock()
					}
				} else {
					// Communication failed, retry next time
					time.Sleep(1000 * time.Millisecond)
				}
				//				}
			}(idx, appendLogArgs, appendLogReply)
		}
	}

	// Wait commit from dominate peers
	<-dominateCommitChan
	replyChannel <- true
}

// Commit logs
func (rf *Raft) commitLogs() {
	for true {
		timer := time.NewTimer(commitLogTimeoutInterval * time.Millisecond)

		select {
		case <-timer.C:
			// If I am leader, I will try to commit logs
			timer.Stop()

			rf.mu.Lock()
			if rf.currentState == leader {
				for index := rf.commitIndex + 1; index <= len(rf.log); index++ {
					commitCount := 0
					for idx, matchedIndex := range rf.matchIndex {
						if matchedIndex >= index || idx == rf.me {
							commitCount++
						}
					}

					if commitCount > len(rf.peers)/2 {
						rf.commitIndex++
					} else {
						break
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

// Apply logs to their local service replica
func (rf *Raft) applyLogs() {
	for true {
		timer := time.NewTimer(applyLogTimeoutInterval * time.Millisecond)

		select {
		case <-timer.C:
			timer.Stop()

			rf.mu.Lock()
			for index := rf.lastApplied; index < rf.commitIndex; index++ {
				fmt.Printf("*applyLogs, server %d, index %d, command %d\n",
					rf.me, index+1, rf.log[index].Command)
				rf.applyCh <- ApplyMsg{index + 1, rf.log[index].Command, false, nil}
				rf.lastApplied++
			}
			rf.mu.Unlock()
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

func (rf *Raft) execute() {
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
							lastLogTerm = rf.log[len(rf.log)-1].Term
						}
						voteArgs := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log),
							lastLogTerm}
						voteReply := new(RequestVoteReply)

						go func(index int, args RequestVoteArgs, reply *RequestVoteReply) {
						RETRY:
							for true {
								res := rf.sendRequestVote(index, args, reply)
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
								} else {
									fmt.Printf("res failed\n")
								}
							}

						}(idx, voteArgs, voteReply)
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

				// Initial nextIndex[] and matchIndex[]
				rf.mu.Lock()
				for idx, _ := range rf.nextIndex {
					rf.nextIndex[idx] = len(rf.log) + 1
				}
				for idx, _ := range rf.matchIndex {
					rf.matchIndex[idx] = 0
				}
				rf.mu.Unlock()
			}
		case leader:
			timer := time.NewTimer(time.Duration(leaderTimeoutInterval) * time.Millisecond)

			select {
			case <-timer.C:
				fmt.Printf("Execute - server:%d, current state:%d send HB to peers\n",
					rf.me, rf.currentState)
				timer.Stop()
				for idx, _ := range rf.peers {
					if idx == rf.me {
						// Skip sending heart beat to myself
					} else {
						preLogIndex := 0
						if len(rf.log) > 1 {
							preLogIndex = len(rf.log) - 1
						}
						preLogTerm := 0
						if len(rf.log) > 1 {
							preLogTerm = rf.log[len(rf.log)-2].Term
						}
						heartBeatArgs := AppendEntriesArgs{rf.currentTerm, rf.me, preLogIndex,
							preLogTerm, nil, rf.commitIndex}
						heartBeatReply := new(AppendEntriesReply)

						go func(index int, args AppendEntriesArgs, reply *AppendEntriesReply) {
							rf.sendAppendEntries(index, args, reply)
						}(idx, heartBeatArgs, heartBeatReply)
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
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.leaderHeartBeatCh = make(chan bool, 1)
	rf.voteGrantedCh = make(chan bool, 1)
	rf.electedAsLeaderCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.execute()
	go rf.commitLogs()
	go rf.applyLogs()

	return rf
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	leaderTimeoutInterval          time.Duration = 100
	commitLogTimeoutInterval       time.Duration = 300
	applyLogTimeoutInterval        time.Duration = 300
)
