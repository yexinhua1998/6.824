package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrInvalidIndex = "ErrInvalidIndex"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Index int    //the index of latest log now. if it is not correct, leader will drop this request.
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err   Err
	Index int //the index of latest log now. no matter of requests is success or fail, it will return the latest index.
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
