package kvraft

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"../labrpc"
)

var ckIdMax = 0
var ckIdMaxMtx sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastIndex int // the last index of lastest req
	ckId      int
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

var listenPprofOnce sync.Once

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	listenPprofOnce.Do(func() {
		go http.ListenAndServe("0.0.0.0:6060", nil)
	})
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	//获取全局唯一的ckId
	ckIdMaxMtx.Lock()
	ck.ckId = ckIdMax
	ckIdMax++
	ckIdMaxMtx.Unlock()

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
		//Index: ck.lastIndex,
	}
	rsp := &GetReply{}
	for {
		for i, svr := range ck.servers {
			ok := svr.Call("KVServer.Get", req, rsp)
			if !ok {
				fmt.Printf("call get not ok")
				continue
			}
			fmt.Printf("client: ckid=%d get svrid=%d key=%s rsp=%+v\n", ck.ckId, i, key, rsp)
			if rsp.Err == ErrWrongLeader {
				continue
			} else if rsp.Err == ErrInvalidIndex {
				//ck.lastIndex = rsp.Index
				continue
			} else if rsp.Err == OK {
				//.lastIndex = rsp.Index
				return rsp.Value
			} else {
				fmt.Printf("Clerk.Get: ckid=%d unexpected error=%s", ck.ckId, rsp.Err)
			}
		}
		time.Sleep(time.Duration(rand.Int63n(1000)) * time.Millisecond)
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

	//因为请求有可能是单向的，即：我发出了请求，我以为你没收到，但事实上你收到了
	//假如你发出了一个请求，你以为你没发送成功，事实上服务端是收到了，并且写进去，然后你重试了，那这个数据就会写两份
	//所以，这里需要确保接口幂等性

	//本质上是因为写入，然后重试了
	//本质问题是：如何重试？

	//看rpc代码，rsp是可能会丢掉的

	//对于一个peer的一次rpc，就要生成一个uuid

	//发现这里如果使用index做幂等的id，会出现频繁的竞争问题（其实这里就是一个典型的乐观并发控制）
	//所以每个cli要等随机的毫秒数才可以重试

	//为什么会把磁盘给打满了？
	//看起来像是内存占用太多了

	// You will have to modify this function.
	req := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	rsp := &PutAppendReply{}
	for {
		for i, svr := range ck.servers {
			req.Index = ck.lastIndex
			ok := svr.Call("KVServer.PutAppend", req, rsp)
			if !ok {
				fmt.Printf("client: ckId=%d call put append not ok\n", ck.ckId)
				continue
			}
			fmt.Printf("client: putappend i=%d op=%s key=%s req=%+v\n", i, op, key, req)
			if rsp.Err == ErrWrongLeader {
				fmt.Printf("client: ckId=%d wrong leader.\n", ck.ckId)
				continue
			} else if rsp.Err == ErrInvalidIndex {
				fmt.Printf("client: ckId=%d invalid index. req.index=%d rsp.index=%d\n", ck.ckId, req.Index, rsp.Index)
				ck.lastIndex = rsp.Index
				continue
			} else if rsp.Err == OK {
				fmt.Printf("client: ckId=%d  putappend OK op=%s key=%s req=%+v rsp=%+v\n", ck.ckId, op, key, req, rsp)
				return
			} else {
				fmt.Printf("client: ckId=%d clerk.PutAppend: unexpected rsp.Err. err=%s\n", ck.ckId, rsp.Err)
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
