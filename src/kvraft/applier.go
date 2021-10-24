package kvraft

import (
	"fmt"
	"sync"

	"../raft"
)

type Applier struct {
	me             int
	ApplyCh        chan raft.ApplyMsg
	Storage        *KvStorage
	MaxIndex       int
	MaxIndexMtx    sync.Mutex
	MaxIndexChange *sync.Cond
}

func NewApplier(me int, applyCh chan raft.ApplyMsg, storage *KvStorage) *Applier {
	applier := &Applier{
		me:       me,
		ApplyCh:  applyCh,
		Storage:  storage,
		MaxIndex: 0,
	}
	applier.MaxIndexChange = sync.NewCond(&applier.MaxIndexMtx)
	return applier
}

func (a *Applier) Start() {
	for {
		applyMsg := <-a.ApplyCh
		op := applyMsg.Command.(Op)
		switch op.OpType {
		case "Put":
			a.Storage.Put(op.Key, op.Value)
		case "Append":
			a.Storage.Append(op.Key, op.Value)
		}
		fmt.Printf("applier %d: applyMsg=%+v storage=%+v\n", a.me, applyMsg, a.Storage)

		//notify MaxIndex被修改，有新的msg被apply
		a.MaxIndexMtx.Lock()
		if applyMsg.CommandIndex > a.MaxIndex {
			a.MaxIndex = applyMsg.CommandIndex
			a.MaxIndexChange.Broadcast()
		} else {
			fmt.Printf("applier %d: unexpected: new command's index lower or equal than max index. applyMsg=%+v maxindex=%d", a.me, applyMsg, a.MaxIndex)
		}
		a.MaxIndexMtx.Unlock()
	}
}

//等待applier将某个index的apply到
func (a *Applier) WaitForIndex(index int) {
	a.MaxIndexMtx.Lock()
	for a.MaxIndex < index {
		a.MaxIndexChange.Wait()
	}
	a.MaxIndexMtx.Unlock()
}
