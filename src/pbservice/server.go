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

  if pb.me == pb.currentview.Primary {
	reply.Value,ok = pb.gostore[args.Key]
	if ok == false {
	  reply.Value = ""
	  reply.Err = ErrNoKey
	  return nil
	} else {
	  reply.Err = OK
	  //forward operations to backup 
	  if pb.currentview.Backup != "" {
	     ok = call(pb.currentview.Backup, "PBServer.AcceptGet", &args, &reply)
	     if ok == false {
		reply.Value = ""
		reply.Err = ErrNoKey
		return nil
	     }
	  }
	  return nil
	}
   } else {
	if pb.currentview.Primary != "" {
	//forward requests to the primary 
	   ok = call(pb.currentview.Primary, "PBServer.AcceptGet", &args, &reply)
	   if ok == true {
		return nil
	   }
        }
	reply.Err = ErrWrongServer
	reply.Value = ""
  }
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  reply.Err = OK
  // Your code here.
  pb.mu.Lock()
  pb.mu.Unlock()
  var ok bool

  if pb.me == pb.currentview.Primary {
	fmt.Printf("Got put request\n")
	pb.gostore[args.Key] = args.Value
	reply.Err = OK
	if pb.currentview.Backup != "" {
	  ok  = call(pb.currentview.Backup, "PBServer.AcceptPut", &args, &reply)
	  if ok == false {
		reply.Err = ErrWrongServer
	  }
	}
  } else {
	reply.Err = ErrWrongServer
	if pb.currentview.Primary != "" {
	  ok = call(pb.currentview.Primary, "PBServer.AcceptPut", &args, &reply)
	  if ok == false {
		reply.Err = ErrWrongServer
	  }
	}
	fmt.Printf("Unable to server put \n")
  }
  return nil
}

//accepting forwarded requests
func (pb *PBServer) AcceptGet(args *GetArgs, reply *GetReply) error {
 pb.mu.Lock()
 defer pb.mu.Unlock()
 var ok bool
 fmt.Printf("Backup/Primary get request \n")
 reply.Value,ok = pb.gostore[args.Key]
 if ok == false {
	reply.Value = ""
	reply.Err = ErrNoKey
 } else {
	reply.Err = OK
 }
 return nil
}

func (pb *PBServer) AcceptPut(args *PutArgs, reply *PutReply) error {
 pb.mu.Lock()
 defer pb.mu.Unlock()
 fmt.Printf("Backup/Primary put request \n")
 pb.gostore[args.Key] = args.Value
 reply.Err = OK
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
  if errx == nil {
	ShouldSyncNewBackup := pb.currentview.Primary == newview.Primary && pb.currentview.Backup != newview.Backup
	pb.currentview = newview
	if ShouldSyncNewBackup {
		pb.updateBackup()
	}
	//pb.currentview = newview
	//fmt.Printf("Got new  view %v me %v \n", pb.currentview.Primary, pb.me)
  }
}

//primary syncs new backup
func (pb *PBServer) updateBackup() error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
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
  fmt.Printf("Backup received request \n")
  pb.gostore = map[string]string {}
  for k, v := range request.KeyValue {
	fmt.Printf("syncing backup \n")
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
