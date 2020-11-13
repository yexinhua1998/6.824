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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//"fmt"
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
	role               int //0-follower 1-candidate 2-leader
	votedFor           int   //last roted for someone.-1 means none of candidate has been voted
	heartBeatTimeOutMs int64 //the time of peer become candidate if there is no heart beat received
	recvHeartBeat 	   bool 
	cond               *sync.Cond //condition variable that signal intter status has changed
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
	if args.CandidateTerm > rf.term {
		//switch to follower
		rf.term = args.CandidateTerm
		rf.role = 0 //follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}else if args.CandidateTerm == rf.term && rf.role == 0 && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	}

	//fmt.Printf("RequestVote,id=%d,role=%d,req=%v,rsp=%v\n",rf.me,rf.role,args,reply)
}

//-----------------implement AppendEntries Service--------------------

type AppendEntriesReq struct {
	LeaderId int
	Term     int
}

type AppendEntriesRsp struct {
}

//2A: implements heartbeats only

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rsp *AppendEntriesRsp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("AppendEntries:role=%d,id=%d\n",rf.role,rf.me)

	if req.Term > rf.term || ( (req.Term == rf.term) && (rf.role == 1) ){
		rf.role = 0 //follower
		rf.term = req.Term
	}
	rf.recvHeartBeat = true
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
	rf.recvHeartBeat = false
	rf.cond = sync.NewCond(&rf.mu)
	go rf.HeartBeatSender(100)
	go rf.ElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//because of this is parralel send heartbeat ,the time is controlable
func (rf *Raft) BroadcastHeartBeat(){
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			var rsp AppendEntriesRsp
			go rf.sendAppendEntries(serverId, &AppendEntriesReq{rf.me,rf.term}, &rsp)
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
func GenRandomInt(upBound int,lowBound int) int{
	if lowBound > upBound {
		//swap
		t := lowBound 
		lowBound = upBound 
		upBound = t
	}
	return rand.Intn( upBound - lowBound ) + lowBound
}

//thread that listening for election timeout
func (rf *Raft) ElectionTimer(){
	for{
		rf.mu.Lock()
		if !rf.recvHeartBeat && rf.role==0 {
			//follower and election time out
			rf.role = 1 //candidate
			go rf.TryToBecomeLeader()
		}
		rf.recvHeartBeat=false
		rf.mu.Unlock()

		electionTimeOut := GenRandomInt(300,500)
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)
	}
}

//function that while peer become candidate been called
func (rf *Raft) TryToBecomeLeader(){
	for{
		rf.mu.Lock()
		serverNum := len(rf.peers)
		term := rf.term
		rf.mu.Unlock()

		var mutex sync.Mutex//access timeOut,ok_num
		var isTimeOut = false 
		var ok_num = 1 //one is the vote from yourself
		var cond = sync.NewCond(&mutex)

		//listen for timeout
		go func(timeout_ms int){
			time.Sleep(time.Duration(timeout_ms) * time.Millisecond)
			mutex.Lock()
			isTimeOut = true 
			mutex.Unlock()
			cond.Signal()
		}(GenRandomInt(300,500))


		for i := 0 ; i < serverNum ; i++{
			if i!=rf.me{
				go func(serverId int){
					var rsp RequestVoteReply
					ok := rf.sendRequestVote(serverId,&RequestVoteArgs{int(term),rf.me},&rsp)
					if ok && rsp.VoteGranted{
						mutex.Lock()
						ok_num = ok_num+1 
						mutex.Unlock()
						cond.Signal()
					}
				}(i)
			}
		}

		mutex.Lock()
		//because of cond.Wait will release the lock,lock will not be locked for a long time
		for !( (serverNum <= 2*ok_num) || isTimeOut ) {
			cond.Wait()
		}

		var isExitLoop = false

		if !isTimeOut {
			rf.role = 2 //leader
			rf.term = rf.term + 1
			isExitLoop = true
		}else if rf.role==0{
			isExitLoop = true
		}else{
			rf.term = rf.term + 1
		}

		mutex.Unlock()

		if isExitLoop {
			break
		}
	}
}