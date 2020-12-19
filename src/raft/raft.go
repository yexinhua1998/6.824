package raft

//SINWARD'S LOG:
//To install vscode go env,we let go module on by shell command below:
//go env -w GO111MODULE=on
//
//but if you run shell 'go test -run 2A' which is the command to test your code
//you will cannot run test,which is a fucking thing.
//you should unset the go module feature by run shell 'go env -u GO111MODULE'

//LOG HERE:
//2020-11-10 TODO:fix the compiling error.let the 'go test -run 2A' show the test result.
//2020-11-11 00:17 DONE: fix the bug of many of peers claim to be leader
//				   TODO: fix the bug of there are leaders while expected no leader
//2020-11-12 20:40 DONE: 在candidate状态下，群发requestsVote，并且根据响应是否成为leader逻辑
//				   TODO: 完成candidate在一定时间内没有成为leader，会重新发起一次选举逻辑
//2020-11-13 01:07 DONE: 重新写了candidate逻辑，和原本的candidate逻辑混在一起居然成功了
//				   TODO: 优化并搞清楚成功的原因
//2020-12-26 00:10 DONE: 大致写完RPC和其他goroutine的逻辑
//                 TODO: 完成Start()中append command的逻辑，修正heartbeat中发送空append entreis请求的逻辑
//2020-12-17 00:35 DONE: 大致完成了syncEntries2Followers()中的逻辑，但同时监听多个事件的逻辑还没有写完
//                 TODO: 用channel+select的方法重写syncEntries2Followers()的逻辑，同时监听多个事件
//2020-12-18 00:48 DONE: 用channel+select的方法大致重写syncEntries2Followers()的逻辑，同时监听多个事件
//                 TODO: 重写Start()逻辑，它是不保证commited且立刻返回的。可以使用直接append进去，通知另一个goroutine去一直消费，确保committed
//2020-12-18 21:35 DONE: 重写整个Start Commit Apply逻辑,PASS了两个测试
//                 TODO: 重写RequestsVote逻辑

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
	"sync/atomic"
	"time"

	//"fmt"
	"encoding/json"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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

	term               int
	role               int   //0-follower 1-candidate 2-leader
	votedFor           int   //last roted for someone.-1 means none of candidate has been voted
	heartBeatTimeOutMs int64 //the time of peer become candidate if there is no heart beat received
	recvHeartBeat      bool
	condRoleChanged    *sync.Cond //condition variable that signal role is changed
	log                []LogEntry
	lastCommitted      int //the index of last committed log entry
	nextIndex          []int

	condAppStartLog *sync.Cond //the condition variable tha signal app add a log to local

	applyCh     chan ApplyMsg
	lastApplied int

	condCommitedIncre *sync.Cond
}

//struct represent of a log entry
type LogEntry struct {
	Term     int
	LogIndex int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = int(rf.term)
	isleader = rf.role == 2 //is leader

	fmt.Printf("GetState():role=%d term=%d isleader=%v\n", rf.role, term, isleader)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int //candidate's term
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	FollowerTerm int  //Follower's term
	VoteGranted  bool //is vote granted
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.term
	reply.VoteGranted = false
	//__STOP_AT_HERE__
	//fix

	if args.CandidateTerm > rf.term {
		//switch to follower
		rf.term = args.CandidateTerm
		rf.role = 0      //follower
		rf.votedFor = -1 //mean null
		rf.condRoleChanged.Broadcast()
	}
	lastLog := rf.log[len(rf.log)-1]
	if rf.votedFor == args.CandidateId || args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.LogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.recvHeartBeat = true
	}

	/*if args.CandidateTerm == rf.term {
		if rf.role == 0 && rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else if rf.role == 1 {
			lastLogEntry := rf.log[len(rf.log)-1]
			if lastLogEntry.Term > args.CandidateTerm || (lastLogEntry.Term == args.CandidateTerm && lastLogEntry.LogIndex > args.LastLogIndex) {
				rf.role = 0 //follower
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			}
		}
	}*/

	fmt.Printf("RequestVote,id=%d,role=%d,req=%s,rsp=%s\n", rf.me, rf.role, toJSON(args), toJSON(reply))
}

//-----------------implement AppendEntries Service--------------------

type AppendEntriesReq struct {
	LeaderID     int
	Term         int // Leader's term
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int

	Debug string
}

type AppendEntriesRsp struct {
	Term    int
	Success bool
}

//2A: implements heartbeats only

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rsp *AppendEntriesRsp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.recvHeartBeat = true
	rsp.Success = false
	rsp.Term = rf.term
	fmt.Printf("AppendEntries:role=%d,id=%d,req=%s\n", rf.role, rf.me, toJSON(req))

	if req.Term >= rf.term {
		oldRole := rf.role
		oldTerm := rf.term
		rf.role = 0 //follower
		rf.term = req.Term
		if oldRole != 0 {
			rf.condRoleChanged.Broadcast()
		}
		if rf.term > oldTerm {
			rf.votedFor = -1 //mean null
		}
	} else {
		return
	}

	logSize := len(rf.log)
	fmt.Printf("id=%d role=%d term=%d log=%s\n", rf.me, rf.role, rf.term, toJSON(rf.log))
	if logSize-1 == req.PrevLogIndex && rf.log[logSize-1].Term == req.PrevLogTerm {

		rf.log = append(rf.log, req.Entries...)
		rsp.Success = true

	} else {

		if logSize-1 > req.PrevLogIndex && rf.lastCommitted < req.PrevLogIndex {
			rf.log = rf.log[:req.PrevLogIndex+1]
			rf.log = append(rf.log, req.Entries...)
			rsp.Success = true
		}

		//leader's log entries cannot append to follower's log
		//wait for next append entries rpc
	}

	logSize = len(rf.log)
	haveCommitedIncrement := false
	for logSize-1 > rf.lastCommitted && req.LeaderCommit > rf.lastCommitted {
		rf.lastCommitted++
		haveCommitedIncrement = true
		fmt.Printf("id=%d role=%d log %d commited\n", rf.me, rf.role, rf.lastCommitted)
	}

	if haveCommitedIncrement {
		//notify applyer to apply command to application
		rf.condCommitedIncre.Signal()
	}

	fmt.Printf("AppendEntries:role=%d,id=%d,rsp=%v\n", rf.role, rf.me, toJSON(rsp))

}

//--------------------------------------------------------------------

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

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, rsp *AppendEntriesRsp) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, rsp)
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
	// Your code here (2B).
	rf.mu.Lock()
	if rf.role != 2 {
		rf.mu.Unlock()
		return -1, -1, false
	}
	term := rf.term
	logSize := len(rf.log)
	logEntry := LogEntry{rf.term, logSize, command}
	rf.log = append(rf.log, logEntry)
	rf.condAppStartLog.Broadcast()
	rf.mu.Unlock()

	fmt.Printf("start a command.log = %s\n", toJSON(logEntry))

	return logSize, term, true
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = 0
	rf.term = 0
	rf.recvHeartBeat = false
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{0, 0, nil}
	rf.lastCommitted = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.condRoleChanged = sync.NewCond(&rf.mu)
	rf.condAppStartLog = sync.NewCond(&rf.mu)

	rf.applyCh = applyCh

	rf.condCommitedIncre = sync.NewCond(&rf.mu)

	rf.lastApplied = 0

	syncIndexChan := make(chan int)

	var serverID int
	for serverID, _ = range rf.peers {
		rf.nextIndex[serverID] = 1
	}
	go rf.HeartBeatSender(100)
	go rf.ElectionTimer()

	for serverID, _ = range rf.peers {
		if serverID != rf.me {
			go rf.syncConsumer(serverID, syncIndexChan)
		}
	}

	go rf.leaderCommitter(syncIndexChan)
	go rf.applier()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//because of this is parralel send heartbeat ,the time is controlable
func (rf *Raft) BroadcastHeartBeat() {
	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			var rsp AppendEntriesRsp
			lastLog := rf.log[len(rf.log)-1]
			req := AppendEntriesReq{rf.me, rf.term, lastLog.LogIndex, lastLog.Term, make([]LogEntry, 0), rf.lastCommitted, "heartbeat"}
			go rf.sendAppendEntries(serverID, &req, &rsp)
		}
	}
}

//sending heartbeat to every server
func (rf *Raft) HeartBeatSender(interval_ms int) {
	for {

		rf.mu.Lock()
		//fmt.Printf("triger heart beat sender.role=%d,id=%d,term=%d\n",rf.role,rf.me,rf.term)
		if rf.role == 2 {
			rf.BroadcastHeartBeat()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(interval_ms) * time.Millisecond)
	}
}

//get random int
func GenRandomInt(upBound int, lowBound int) int {
	if lowBound > upBound {
		//swap
		t := lowBound
		lowBound = upBound
		upBound = t
	}
	return rand.Intn(upBound-lowBound) + lowBound
}

//thread that listening for election timeout
func (rf *Raft) ElectionTimer() {
	for {
		electionTimeOut := GenRandomInt(300, 500)
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)

		rf.mu.Lock()
		if !rf.recvHeartBeat && rf.role == 0 {
			//follower and election time out
			rf.role = 1 //candidate
			rf.term++
			go rf.TryToBecomeLeader()
		}
		rf.recvHeartBeat = false
		rf.mu.Unlock()

	}
}

//function that while peer become candidate been called
func (rf *Raft) TryToBecomeLeader() {
	for {
		rf.mu.Lock()
		serverNum := len(rf.peers)
		term := rf.term
		rf.mu.Unlock()

		var mutex sync.Mutex //access timeOut,okNum
		var isTimeOut = false
		var okNum = 1 //one is the vote from yourself
		var cond = sync.NewCond(&mutex)

		//listen for timeout
		go func(timeout_ms int) {
			time.Sleep(time.Duration(timeout_ms) * time.Millisecond)
			mutex.Lock()
			isTimeOut = true
			mutex.Unlock()
			cond.Signal()
		}(GenRandomInt(300, 500))

		for i := 0; i < serverNum; i++ {
			if i != rf.me {
				go func(serverId int) {
					var rsp RequestVoteReply
					lastLog := rf.log[len(rf.log)-1]
					req := RequestVoteArgs{term, rf.me, lastLog.LogIndex, lastLog.Term}
					ok := rf.sendRequestVote(serverId, &req, &rsp)
					if ok && rsp.VoteGranted {
						mutex.Lock()
						okNum++
						mutex.Unlock()
						cond.Signal()
					}
				}(i)
			}
		}

		mutex.Lock()
		//because of cond.Wait will release the lock,lock will not be locked for a long time
		for !((serverNum <= 2*okNum) || isTimeOut) {
			cond.Wait()
		}

		var isExitLoop = false

		if !isTimeOut {
			rf.role = 2 //leader
			for serverID, _ := range rf.peers {
				rf.nextIndex[serverID] = len(rf.log)
			}
			isExitLoop = true
		} else if rf.role == 0 {
			isExitLoop = true
		} else {
			rf.term++
		}

		mutex.Unlock()

		if isExitLoop {
			break
		}
	}
}

func intMin(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func toJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

//listen to commited index and apply to leader's application
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !(rf.lastCommitted > rf.lastApplied) {
			rf.condCommitedIncre.Wait()
		}
		//rf.lastCommitted > rf.lastApplied
		commandIndex := rf.lastApplied + 1
		rf.applyCh <- ApplyMsg{true, rf.log[commandIndex].Command, commandIndex}
		fmt.Printf("id=%d role=%d log %d applied\n", rf.me, rf.role, commandIndex)
		rf.lastApplied++
		rf.mu.Unlock()
	}
}

//commit leader's log if it is replicated in majority of peers
func (rf *Raft) leaderCommitter(syncIndexChan chan int) {
	replicatedNum := make([]int, 1, 1024)
	for {
		syncIndex := <-syncIndexChan
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != 2 {
				return
			}
			for syncIndex > len(replicatedNum)-1 {
				replicatedNum = append(replicatedNum, 1) //1 is leader itself
			}
			//syncIndex <= len(replicatedNum)-1
			replicatedNum[syncIndex]++

			if syncIndex != rf.lastCommitted+1 {
				return
			}
			//syncIndex == rf.lastCommitted+1
			if 2*replicatedNum[syncIndex] >= len(rf.peers) {
				rf.lastCommitted++
				rf.condCommitedIncre.Signal()
				fmt.Printf("id=%d role=%d log %d commited\n", rf.me, rf.role, rf.lastCommitted)
			}
		}()

	}
}

//sync logs to serverID
func (rf *Raft) syncConsumer(serverID int, syncIndexChan chan int) {
	fmt.Printf("start syncConsumer(%d) in %d\n", serverID, rf.me)
	for {
		rf.mu.Lock()
		oldLogLen := len(rf.log)
		for !(oldLogLen < len(rf.log)) {
			fmt.Printf("syncConsumer(%d) in %d:Waiting for cv\n", serverID, rf.me)
			rf.condAppStartLog.Wait()
		}
		fmt.Printf("syncConsumer(%d) in %d:run\n", serverID, rf.me)

		//app add log to raft.Start to sync log to followers
		syncIndex := len(rf.log) - 1
		role := rf.role
		rf.mu.Unlock()
		if role != 2 {
			continue
		}

		var done = make(chan int)

		//listen for role become not leader
		roleBecomeNotLeader := make(chan int)
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.role == 2 {
				select {
				case <-done:
					return
				default:
				}
				rf.condRoleChanged.Wait()
			}
			//rf.role != 2
			roleBecomeNotLeader <- 0
		}()

		synced := make(chan bool)
		go func() {
			success := rf.syncEntries2Follower(serverID, done)
			synced <- success
		}()

		select {
		case <-roleBecomeNotLeader:
			close(done)
		case success := <-synced:
			if success {
				syncIndexChan <- syncIndex
			}
		}
	}
}

//sync leader's entries to followers
func (rf *Raft) syncEntries2Followers(commited chan bool) {

	var okNum = 1 //one is log in yourself
	var failNum = 0
	var done = make(chan int)
	var syncResult = make(chan bool)
	var becomeFollower = make(chan bool)

	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go func(serverID int) {
				//call syncEntries2Follower parallely and send the signal to father goroutine
				success := rf.syncEntries2Follower(serverID, done)
				syncResult <- success
			}(serverID)
		}
	}

	//listen for role changed
	go func() {
		rf.mu.Lock()
		for rf.role == 2 {
			rf.condRoleChanged.Wait()
			select {
			case <-done: //father goroutine is left
				return
			default:
				continue
			}
		}
		becomeFollower <- true
		rf.mu.Unlock()
	}()

	select {

	case success := <-syncResult:
		if success {
			okNum++
			if 2*okNum >= len(rf.peers) {
				//log replicated to majority of peers
				commited <- true
				close(done)
			}
		} else {
			failNum++
			if 2*failNum >= len(rf.peers) {
				commited <- false
				close(done)
			}
		}

	case <-becomeFollower:
		commited <- false
		return

	}
}

func (rf *Raft) syncEntries2Follower(serverID int, done chan int) bool {
	for {
		select {
		case <-done:
			return false
		default:
		}
		nextIndex := rf.nextIndex[serverID]
		prevLog := rf.log[nextIndex-1]
		req := AppendEntriesReq{rf.me, rf.term, prevLog.LogIndex, prevLog.Term, rf.log[nextIndex:], rf.lastCommitted, "sync"}
		rsp := AppendEntriesRsp{}
		ok := rf.sendAppendEntries(serverID, &req, &rsp)
		if !ok {
			return false
		}
		if rsp.Success {
			rf.nextIndex[serverID] = len(rf.log)
			return true
		} else {
			if rf.nextIndex[serverID] > 1 {
				rf.nextIndex[serverID]--
				continue
			} else {
				return false
			}
		}
	}
}
