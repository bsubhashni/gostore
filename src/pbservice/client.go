package pbservice

import "viewservice"
import "net/rpc"
// You'll probably need to uncomment this:
import "time"
import "sync"

type Clerk struct {
  vs *viewservice.Clerk
  view viewservice.View
  mu sync.Mutex
}

func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  ck.view = viewservice.View { 0, "", ""}
  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
   // Your code here. 
   ck.mu.Lock()
   defer ck.mu.Unlock()
   var request GetArgs
   //dummy
   reply := GetReply {OK, ""}
   request.Key = key
   if  ck.view.Primary == "" {
	ck.GetView()
   }
   ok := call(ck.view.Primary,"PBServer.Get",&request,&reply)
   for ok == false ||
	reply.Err == ErrWrongServer {
	   ck.GetView()
	   ok = call(ck.view.Primary,"PBServer.Get",&request,&reply)
   }
  return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
  // Your code here.
  ck.mu.Lock()
  defer ck.mu.Unlock()
  var request PutArgs
  reply := PutReply { OK }
  request = PutArgs {key, value}
  if  ck.view.Primary == "" {
        ck.GetView()
   }
  ok := call(ck.view.Primary, "PBServer.Put",&request, &reply)
  for ok == false ||
	reply.Err == ErrWrongServer {
	ck.GetView()
	ok = call(ck.view.Primary, "PBServer.Put", &request, &reply)
  }
  //return ""
}

func (ck *Clerk) GetView()  {
	var currentview viewservice.View
	var ok bool
	currentview, ok =  ck.vs.Get()
	for ok == false ||
		currentview.Primary == ""  {

			time.Sleep(viewservice.PingInterval)
			currentview,ok = ck.vs.Get()
	}
	ck.view = currentview
}

