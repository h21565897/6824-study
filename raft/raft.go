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
	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	serverNum int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentState int
	currentTerm  int
	voteFor      int
	log          []Entry

	//volatile state
	commitIndex int
	lastApplied int

	timeoutTicker *time.Ticker
	tickerReset   chan int

	//volatile states on leaders
	nextIndex  []int
	matchIndex []int

	appendEntryResquests chan *AppendEntryTemplate
	applych              chan ApplyMsg
	commitedIndexCount   map[int]int
	lastNewEntry         map[int]int
	r                    *rand.Rand
	lower                int
	upper int
}

var (
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3
)

type Entry struct {
	Command interface{}
	LogTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.currentState == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	//Your code here (2C).
	//Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		log.Printf("Server %v recover fail!\n", rf.me)
	} else {
		rf.log = logs
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curloglen := len(rf.log)
	//log.Printf("recevied vote request from SERVER: %v, CURRENTSERVER:%v ARGS TERM:%v CURRENTTERM:%v LastLogterm %v,CurrentLastLogTerm%v LastIndex%v,CurrentLastLogIndex %v\n",
	//	args.CandidateId, rf.me, args.Term, rf.currentTerm, args.LastLogTerm, rf.log[curloglen-1].LogTerm, args.LastLogIndex, curloglen-1)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.voteFor = -1
		log.Printf("CURRENSERVER %v,grant vote for server %v\n", rf.me, args.CandidateId)
	}
	if rf.log[curloglen-1].LogTerm > args.LastLogTerm ||
		(rf.log[curloglen-1].LogTerm == args.LastLogTerm && curloglen-1 > args.LastLogIndex) {
		return
	}
	if rf.voteFor == args.CandidateId || rf.voteFor == -1 {
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		go rf.resetTimeOutTicker()
	}
	rf.persist()
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
	// Your code here (2B).
	term := rf.currentTerm
	isLeader := (rf.currentState == LEADER)
	index := -1
	if isLeader {
		log.Printf("Start %v\n", command)
		newEntry := Entry{
			Command: command,
			LogTerm: rf.currentTerm,
		}
		rf.log = append(rf.log, newEntry)
		rf.persist()
		log.Printf("LOG AT LEADER %v NOW IS %v %v\n", rf.me, len(rf.log), rf.log)
		//暂时的 新log comes in 直接发一个请求 todo: use ticker and send log every 200 ms to improve proficiency
		endIndex := len(rf.log)
		index = endIndex - 1
		for k, v := range rf.nextIndex {
			if rf.me != k {
				if len(rf.log) >= v {
					//传endindex主要是方便最后commit回调的时候同步，其实可以在下面那个goroutine里面写
					go rf.makeAppendEntryRequest(k, v, endIndex)
				}
			}
		}
	}
	return index, term, isLeader
}

func (rf *Raft) makeAppendEntryRequest(server int, startIndex int, endIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//the response maybe too slow
	// if rf.currentState != LEADER {
	// 	return
	// }
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  rf.log[startIndex-1].LogTerm,
		Entries:      rf.log[startIndex:endIndex],
		LeaderCommit: rf.commitIndex,
	}
	//rf.nextIndex[server] = endIndex
	//log.Printf("LOG is being sent from SERVER  %v to CURRENSERVER %v,START %v, END %v,and entries is %v\n", rf.me, server, startIndex, endIndex, args.Entries)
	go rf.sendAppendEntryReqeust(&AppendEntryTemplate{
		args:       args,
		server:     server,
		startIndex: startIndex,
		endIndex:   endIndex,
	})
}

type AppendEntryTemplate struct {
	args       *AppendEntriesArgs
	server     int
	startIndex int
	endIndex   int
}

func (rf *Raft) sendAppendEntryReqeust(template *AppendEntryTemplate) {
	rf.appendEntryResquests <- template
}
func (rf *Raft) handleAppendEntryRequest() {
	for rf.killed() == false {
		select {
		case template := <-rf.appendEntryResquests:
			go rf.doAppendEntriesForLog(template)
		}
	}
}
func (rf *Raft) doAppendEntriesForLog(template *AppendEntryTemplate) {
	reply := &AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	server := template.server
	args := template.args
	startIndex := template.startIndex
	endIndex := template.endIndex
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//不合法 直接return
	if args.Term != rf.currentTerm || rf.currentState != LEADER {
		return
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.currentState = FOLLOWER
			rf.persist()
			return
		}
		if reply.Success {
			canBeCommited := (rf.serverNum - 1) / 2
			rf.nextIndex[server] = endIndex //endindex开
			rf.matchIndex[server] = endIndex - 1
			rf.commitedIndexCount[endIndex-1]++
			if rf.commitedIndexCount[endIndex-1] >= canBeCommited && endIndex-1 > rf.commitIndex && rf.log[endIndex-1].LogTerm == rf.currentTerm {
				log.Printf("log commit at return\n")
				rf.newFunction(rf.commitIndex+1, endIndex-1)
				rf.commitIndex = endIndex - 1
			}
		} else {
			//如果发送失败 并且起点仍然大于当前的下一个 next减一继续发
			if startIndex >= rf.nextIndex[server] {
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
					log.Printf("too many logs CURRENSERVER %v,next start at %v prevlogs %v now logs %v\n", server, rf.nextIndex[server], template.args.Entries, rf.log[rf.nextIndex[server]:])
					go rf.makeAppendEntryRequest(server, rf.nextIndex[server], len(rf.log)-1)
				} else {
					log.Printf("too many logs CURRENSERVER %v,next start at %v logs %v\n", server, 1, template.args.Entries)
					go rf.makeAppendEntryRequest(server, 1, len(rf.log)-1)
				}
			}
		}
	} else {
		if startIndex >= rf.nextIndex[server] {
			log.Printf("resubmit log to CURRENSERVER %v from sever %v logs : %v \n", server, rf.me, template.args.Entries)
			go rf.makeAppendEntryRequest(server, startIndex, endIndex)
		}
	}
}

func (rf *Raft) newFunction(startIndex int, endIndex int) {
	for i := startIndex; i <= endIndex; i++ {
		log.Printf("log %v %v is now commited at CURRENSERVER %v %v\n", i, rf.log[i], rf.me, rf.currentState)
		rf.applych <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[i].Command,
			CommandIndex:  i,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
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
	log.Printf("server %v is down\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) calculateSleepTime() int {
	upper := rf.upper
	lower := rf.lower
	return rf.r.Intn(upper-lower) + lower
}
func (rf *Raft) calculateSleepTimeDuration() time.Duration {
	upper := rf.upper
	lower := rf.lower
	return time.Duration(rf.r.Intn(upper-lower)+lower) * time.Millisecond
}
func (rf *Raft) resetTimeOutTicker() {
	rf.tickerReset <- 1
}
func (rf *Raft) resetTimeOutTickerTo(duration int) {
	rf.tickerReset <- duration
	//fmt.Print("hehe")
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		var t int
		select {
		case t = <-rf.tickerReset:
			if t == 1 {
				duration := rf.calculateSleepTimeDuration()
				rf.timeoutTicker.Reset(duration)
				//log.Printf("server %v ticker is reset to %v\n", rf.me, duration)
			} else {
				rf.timeoutTicker.Reset(time.Duration(t) * time.Millisecond)
			}
		case <-rf.timeoutTicker.C:
			rf.mu.Lock()
			cur := rf.currentState
			isLeader := (cur == LEADER)
			rf.mu.Unlock()
			duration := rf.calculateSleepTime()
			if !isLeader {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Millisecond)
				switch cur {
				case FOLLOWER:
					go rf.startAnElection(ctx, cancel)
				case CANDIDATE:
					go rf.startAnElection(ctx, cancel)
				}
			}
			go rf.resetTimeOutTickerTo(duration)
		default:
			// 	time.Sleep(50 * time.Millisecond)
		}
	}
}

func (rf *Raft) initialHeartBeatTicker(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentState != LEADER {
			return
		}
		//	log.Printf("HeartTick is being sent from %v, to %v,and term is %v\n", rf.me, server, rf.currentTerm)
		if reply.Term > rf.currentTerm {
			//log.Printf("Server %v is going back to follow state!!!!!!!!!!\n", rf.me)
			rf.currentTerm = reply.Term
			rf.currentState = FOLLOWER
			rf.persist()
			return
		}
	}
}

func (rf *Raft) startHeartBeatTicker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for rf.killed() == false {
		<-ticker.C
		if rf.killed() {
			return
		}
		if rf.currentState == LEADER {
			//rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			//rf.mu.Unlock()
			for k, _ := range rf.peers {
				if k != rf.me {
					//	log.Printf("HeartTick is trying to send from %v, to %v,and term is %v\n", rf.me, k, rf.currentTerm)
					go rf.initialHeartBeatTicker(k, &args)
				}
			}
		}
	}
}
func (rf *Raft) initalARequestVote(ctx context.Context, votechan chan int, cancel context.CancelFunc, server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if rf.sendRequestVote(server, args, &reply) {
		select {
		case <-ctx.Done():
			return
		default:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentState = FOLLOWER
				rf.voteFor = server
				go rf.resetTimeOutTicker()
				rf.persist()
				cancel()
			}
			rf.mu.Unlock()
		}
		if reply.VoteGranted {
			votechan <- 1
		}
	}

}
func (rf *Raft) startAnElection(ctx context.Context, cancel context.CancelFunc) {
	rf.mu.Lock()
	rf.currentTerm++
	log.Printf("server %v start an election,term %v \n", rf.me, rf.currentTerm)
	rf.currentState = CANDIDATE
	rf.voteFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].LogTerm,
	}
	rf.mu.Unlock()
	voteNums := 1
	canBeLeader := (rf.serverNum - 1) / 2
	voteChan := make(chan int)
	for k, _ := range rf.peers {
		if k != rf.me {
			//log.Printf("server %v is try to send a vote to server %v args%v \n", rf.me, k, args)
			go rf.initalARequestVote(ctx, voteChan, cancel, k, &args)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-voteChan:
			voteNums++
			if voteNums > canBeLeader {
				log.Printf("server %v wins the election!\n", rf.me)
				rf.mu.Lock()
				rf.currentState = LEADER
				in := len(rf.log)
				for k, _ := range rf.nextIndex {
					rf.nextIndex[k] = in
				}
				for k, _ := range rf.matchIndex {
					rf.matchIndex[k] = 0
				}
				rf.commitedIndexCount = make(map[int]int)
				rf.mu.Unlock()
				return
			}
		}
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	go rf.resetTimeOutTicker()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.persist()
	}
	if args.Entries != nil {
		//log.Printf("Received append request from SERVER %v CURRENSERVER %v,received entries %v,received prevs %v %v,current logs%v,log length %v\n",
		//args.LeaderId, rf.me, args.Entries, args.PrevLogIndex, args.PrevLogTerm, rf.log, len(rf.log))

	}
	reply.Term = rf.currentTerm
	if rf.commitIndex < args.LeaderCommit {
		//如果当前的term合法 才进行提交
		var commitIndex int
		if args.LeaderCommit < rf.lastNewEntry[rf.currentTerm] {
			commitIndex = args.LeaderCommit
		} else {
			commitIndex = rf.lastNewEntry[rf.currentTerm]
		}
		//if rf.log[commitIndex].LogTerm == rf.currentTerm {
		//log.Printf("commit at append %v %v\n", args.Term, rf.currentTerm)
		rf.newFunction(rf.commitIndex+1, commitIndex)
		rf.commitIndex = commitIndex
		//}
	}
	//log not match,just return it
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		reply.Success = false
		//rf.log = rf.log[0:args.PrevLogIndex]
		return
	}

	if args.Entries != nil {
		//maybe too long because of fail
		j := 0
		i := args.PrevLogIndex + 1
		loglen := len(rf.log)
		entrieslen := len(args.Entries)
		for i < loglen && j < entrieslen && rf.log[i].LogTerm == args.Entries[j].LogTerm {
			i++
			j++
		}
		//truncate any logs conflicts with the leader
		if j < entrieslen {
			rf.log = rf.log[0:i]
			rf.log = append(rf.log, args.Entries[j:]...)
			//rf.persist()
		}
		rf.lastNewEntry[rf.currentTerm] = len(rf.log) - 1
		log.Printf("logs append successfully in CURRENSERVER %v,current Log is%v\n", rf.me, rf.log)
	}
	rf.persist()
	reply.Success = true

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	//peers all the clients
	//me index to this raft's client
	//persister about persisting data
	//applymsg  channel to apply a msg to kv
	//a method called applier will indefinitely check the applymsg to ensure the msg is right and append it to the log
	rf := &Raft{}
	//rf.dead = 0
	log.Printf("Server %v start\n", me)
	rf.applych = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.serverNum = len(rf.peers)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{
		Command: nil,
		LogTerm: 0,
	})
	rf.nextIndex = make([]int, rf.serverNum)
	rf.matchIndex = make([]int, rf.serverNum)
	rf.commitIndex = 0
	rf.commitedIndexCount = make(map[int]int)
	rf.appendEntryResquests = make(chan *AppendEntryTemplate)
	rf.currentState = FOLLOWER
	rf.tickerReset = make(chan int)
	source := rand.NewSource(int64(rand.Int63())+int64(me)*200000000)
	rf.r = rand.New(source)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lower = 150 + me*100
	rf.upper=rf.lower+150
	// start ticker goroutine to start elections
	rf.timeoutTicker = time.NewTicker(rf.calculateSleepTimeDuration())
	rf.lastNewEntry = make(map[int]int)
	go rf.ticker()
	go rf.startHeartBeatTicker()
	go rf.handleAppendEntryRequest()

	return rf
}
