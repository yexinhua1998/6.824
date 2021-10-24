package kvraft

import (
	"fmt"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//从leader处执行GET操作

	// You will have to modify this function.
	req := &GetArgs{
		Key: key,
	}
	rsp := &GetReply{}
	for {
		for i, svr := range ck.servers {
			ok := svr.Call("KVServer.Get", req, rsp)
			if !ok {
				fmt.Printf("call get not ok")
				continue
			}
			fmt.Printf("client: get i=%d key=%s rsp=%+v\n", i, key, rsp)
			if rsp.Err == OK {
				//fmt.Printf("client: get key=%s value=%s", key, rsp.Value)
				return rsp.Value
			}
		}
		time.Sleep(time.Second)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//对Leader执行PUT操作

	// You will have to modify this function.
	req := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	rsp := &PutAppendReply{}
	for {
		for i, svr := range ck.servers {
			ok := svr.Call("KVServer.PutAppend", req, rsp)
			if !ok {
				fmt.Printf("call put append not ok")
				continue
			}
			fmt.Printf("client: putappend i=%d op=%s key=%s req=%+v\n", i, op, key, req)
			if rsp.Err == OK {
				fmt.Printf("client: putappend OK op=%s key=%s req=%+v rsp=%+v\n", op, key, req, rsp)
				return
			}
		}
		time.Sleep(time.Second)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
