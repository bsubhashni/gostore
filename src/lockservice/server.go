package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type LockStatus struct {
   status bool
   clientid int64
   requestid int64
}

type ResponseStore struct {
  hasSeenRequest  bool
  lastResponse bool
}

type LockServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool  // for test_test.go
  dying bool // for test_test.go

  am_primary bool // am I the primary?
  backup string   // backup's port

  // for each lock name, is it locked?
  locks map[string]LockStatus
  servedrequests map[int64]ResponseStore
}


//
// server Lock RPC handler.
//
// you will have to modify this function
//
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()
  if ls.servedrequests[args.Requestid].hasSeenRequest == true {
	reply.OK = ls.servedrequests[args.Requestid].lastResponse
	return nil
  }
  lockstatus, _ := ls.locks[args.Lockname]
  locked := lockstatus.status

  if locked == true {
     reply.OK = false
   } else {
    newlockstatus := LockStatus { true,  args.Clientid, args.Requestid }
    ls.locks[args.Lockname] = newlockstatus
    reply.OK = true
   }

   if ls.am_primary == true {
    var backupreply LockReply
    c, errx := rpc.Dial("unix", ls.backup)
    if errx != nil {
	 //backup conenction failed
    } else {
     defer c.Close()
     ok := c.Call("LockServer.Lock", args, &backupreply)
     if ok == nil {
	if backupreply.OK != reply.OK {
        	//inconsistent data
	}
      } else {
	//backup failed
     }
    }
   }
  ls.servedrequests[args.Requestid] = ResponseStore { true, reply.OK }
  return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()

  if ls.servedrequests[args.Requestid].hasSeenRequest == true {
	reply.OK = ls.servedrequests[args.Requestid].lastResponse
	return nil
  }

  lockstatus, _ :=  ls.locks[args.Lockname]
  locked := lockstatus.status

   if locked == false {
     reply.OK = false
    } else  {
     newlockstatus := LockStatus { false, args.Clientid, args.Requestid }
     ls.locks[args.Lockname] = newlockstatus
     reply.OK = true
    }

    if ls.am_primary == true {
     var backupreply UnlockReply
     c, errx := rpc.Dial("unix", ls.backup)
     if errx != nil {
	//backup connection failed
     } else {
      defer c.Close()
      ok := c.Call("LockServer.Unlock", args, &backupreply)
      if ok == nil {
	if backupreply.OK != reply.OK {
		//inconsistent data
	}
      } else {
	//backup failed
      }
     }
   }

  ls.servedrequests[args.Requestid] = ResponseStore { true, reply.OK }
  return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
  ls.dead = true
  ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
  c io.ReadWriteCloser
}
func (dc DeafConn) Write(p []byte) (n int, err error) {
  return len(p), nil
}
func (dc DeafConn) Close() error {
  return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
  return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
  ls := new(LockServer)
  ls.backup = backup
  ls.am_primary = am_primary
  ls.locks = map[string]LockStatus{}

  // Your initialization code here.
  ls.servedrequests = map[int64]ResponseStore{}

  me := ""
  if am_primary {
    me = primary
  } else {
    me = backup
  }

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(ls)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(me) // only needed for "unix"
  l, e := net.Listen("unix", me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  ls.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for ls.dead == false {
      conn, err := ls.l.Accept()
      if err == nil && ls.dead == false {
        if ls.dying {
          // process the request but force discard of reply.

          // without this the connection is never closed,
          // b/c ServeConn() is waiting for more requests.
          // test_test.go depends on this two seconds.
          go func() {
            time.Sleep(2 * time.Second)
            conn.Close()
          }()
          ls.l.Close()

          // this object has the type ServeConn expects,
          // but discards writes (i.e. discards the RPC reply).
          deaf_conn := DeafConn{c : conn}

          rpcs.ServeConn(deaf_conn)

          ls.dead = true
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && ls.dead == false {
        fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
        ls.kill()
      }
    }
  }()

  return ls
}
