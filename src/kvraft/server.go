package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage *KvStorage
	applier *Applier
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//判断自己是否为leader
	//如果是，从自己维护的kv状态中读取信息
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	value := kv.storage.Get(args.Key)
	//TODO: debug为什么rpc拿到的req是空的？
	fmt.Printf("server: get. me=%d req=%+v key=%s value=%s\n", kv.me, args, args.Key, value)
	reply.Value = value
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//将对应的command rf.Start()进去。如果返回不是leader，则将结果返回给client
	//开启一个backgroud goroutine，监听rf.applyCh，并自己维护kv状态

	//无论成功还是失败，都要返回真实的log最后的index
	defer func() {
		reply.Index = kv.rf.LogSize() - 1
	}()

	//要先检查是否是leader，不是leader直接返回
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	//如果请求的Index不等于当前raft log最后的entry的index，则说明这个请求可能是重复的，需要抛弃
	if args.Index != kv.rf.LogSize()-1 {
		reply.Err = ErrInvalidIndex
		return
	}

	op := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}
	fmt.Printf("server: putappend. key=%s value=%s\n", args.Key, args.Value)
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
	} else {
		//等待applier通知
		fmt.Printf("waiting for index...\n")
		kv.applier.WaitForIndex(index)
		fmt.Printf("wait done.\n")
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = NewKvStorage()

	//applier := &Applier{
	//	me:      me,
	//	ApplyCh: kv.applyCh,
	//	Storage: kv.storage,
	//}
	applier := NewApplier(me, kv.applyCh, kv.storage)
	kv.applier = applier

	go applier.Start()

	return kv
}
