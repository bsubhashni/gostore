package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
  Viewnum uint
}

type GetReply struct {
  Err Err
  Value string
}

type GoStore struct {
  KeyValue map[string]string
}

type SyncReply struct {
  Done bool
}

type AcceptGetArgs struct {
   Key string
   Viewnum uint
}
type AcceptPutArgs struct {
   Key string
   Value string
   Viewnum uint
}


// Your RPC definitions here.
