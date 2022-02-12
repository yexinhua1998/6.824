package raft

import (
	"log"
	"sync"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//用于使用channel来做广播
//避开conditional variable
//这样可以同时监听done和heartbeat
type Broadcaster struct {
	mu         sync.Mutex
	chanMap    map[int]chan interface{}
	nextChanId int //用于生成唯一id
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		chanMap: make(map[int]chan interface{}),
	}
}

//返回用来接受广播的channel和释放的func
func (bc *Broadcaster) GetChan() (<-chan interface{}, func()) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	listenChan := make(chan interface{})
	chanId := bc.nextChanId
	bc.chanMap[chanId] = listenChan
	bc.nextChanId++
	return listenChan, func() {
		bc.mu.Lock()
		defer bc.mu.Unlock()
		delete(bc.chanMap, chanId)
	}
}

//广播
//中途除了mutex，不会阻塞
func (bc *Broadcaster) Broadcast() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for _, listenChan := range bc.chanMap {
		select {
		case listenChan <- nil:
		default:
		}
	}
}
