package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  // Your declarations here.
  gostore map[string]string
  currentview viewservice.View
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  var ok bool
  var backupargs AcceptGetArgs
  var backupreply GetReply

  if pb.me == pb.currentview.Primary {
	reply.Value,ok = pb.gostore[args.Key]
	if ok == false {
	  reply.Value = ""
	  reply.Err = ErrNoKey
	} else {
	  reply.Err = OK
	}
	if pb.currentview.Backup != "" {
	  backupargs = AcceptGetArgs { args.Key, pb.currentview.Viewnum  }
	  //forward operations to backup
	  ok = call(pb.currentview.Backup, "PBServer.AcceptGet", &backupargs, &backupreply)
	  if ok != false &&
		backupreply.Err != OK  {
		reply.Err =ErrWrongServer
		reply.Value = ""
	  } else if ok == false {
		reply.Err = ErrWrongServer
		reply.Value = ""
	  }
	}
   } else {
/*	if pb.currentview.Primary != "" {
	//forward requests to the primary 
	   ok = call(pb.currentview.Primary, "PBServer.AcceptGet", &args, &reply)
        }*/
	reply.Err = ErrWrongServer
	reply.Value = ""
  }
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  reply.Err = OK
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  var ok bool
  var backupargs AcceptPutArgs
  if pb.me == pb.currentview.Primary {
	pb.gostore[args.Key] = args.Value
	reply.Err = OK
	if pb.currentview.Backup != "" {
	  backupargs = AcceptPutArgs {args.Key, args.Value, pb.currentview.Viewnum }
	  ok  = call(pb.currentview.Backup, "PBServer.AcceptPut", &backupargs, &reply)
	  if ok != false &&
			reply.Err != OK {
		reply.Err = ErrWrongServer
	  } else if ok == false {
		delete(pb.gostore, args.Key)
		reply.Err = ErrWrongServer
	  }
	}
  } else {
	/*if pb.currentview.Primary != "" {
	  ok = call(pb.currentview.Primary, "PBServer.AcceptPut", &args, &reply)
	}*/
	reply.Err = ErrWrongServer
  }
  return nil
}

//accepting forwarded requests
func (pb *PBServer) AcceptGet(args *AcceptGetArgs, reply *GetReply) error {
 pb.mu.Lock()
 defer pb.mu.Unlock()
 var ok bool
 if pb.currentview.Backup == pb.me &&
	pb.currentview.Viewnum == args.Viewnum  {
    reply.Value,ok = pb.gostore[args.Key]
    if ok == false {
	  reply.Value = ""
	  reply.Err = ErrNoKey
	} else {
	 reply.Err = OK
	}
 } else {
    reply.Err = ErrWrongServer
 }
 return nil
}

func (pb *PBServer) AcceptPut(args *AcceptPutArgs, reply *PutReply) error {
 pb.mu.Lock()
 defer pb.mu.Unlock()
 if pb.currentview.Backup == pb.me &&
	pb.currentview.Viewnum == args.Viewnum {
    pb.gostore[args.Key] = args.Value
    reply.Err = OK
 } else {
  reply.Err = ErrWrongServer
 }
 return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  // Your code here.
  var newview viewservice.View
  newview,errx := pb.vs.Ping(pb.currentview.Viewnum)
  if errx == nil &&
	pb.currentview.Viewnum != newview.Viewnum {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	ShouldSyncNewBackup := pb.currentview.Primary == newview.Primary && pb.currentview.Primary == pb.me && pb.currentview.Backup != newview.Backup && newview.Backup != ""
	pb.currentview = newview
	if ShouldSyncNewBackup {
		pb.updateBackup()
	}
  }
}

//primary syncs new backup
func (pb *PBServer) updateBackup() error {
  request := GoStore { pb.gostore }
  reply := SyncReply {}
  ok := call(pb.currentview.Backup, "PBServer.Sync",&request, &reply)
  if ok == false {
	fmt.Printf("Backup sync failed from Primary \n")
  }
  return nil
}
func (pb *PBServer) Sync(request *GoStore, reply *SyncReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  pb.gostore = map[string]string {}
  for k, v := range request.KeyValue {
	pb.gostore[k] = v
  }
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  // Your pb.* initializations here.
  pb.gostore = map[string]string {}
  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
