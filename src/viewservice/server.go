package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "errors"
type ServerPingInfo struct {
  lastpingedtime time.Time
  lastviewnumseen uint
}

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  viewuninitialized bool
  viewbackupuninitialized bool
  backupAcked bool
  lastackedview uint
  primarychanged bool
  currentview View
  newview View
  serverscount int
  servers map[string]ServerPingInfo
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  // Your code here.
  hasViewChanged := false
  isViewEmpty := vs.currentview.Primary == "" && vs.currentview.Backup == "" && vs.serverscount == 0 && vs.viewuninitialized == true
  isBackupNotSet := vs.currentview.Primary != "" && vs.currentview.Backup == "" && vs.serverscount == 1  && vs.currentview.Primary != args.Me && vs.viewbackupuninitialized == true

  if isViewEmpty {
	vs.currentview.Primary = args.Me
	vs.currentview.Viewnum += 1
	vs.serverscount += 1
	vs.viewuninitialized = false
  } else if isBackupNotSet {
	vs.newview.Primary = vs.currentview.Primary
	vs.newview.Backup = args.Me
	vs.serverscount += 1
	hasViewChanged = true
	vs.backupAcked = false
	vs.viewbackupuninitialized = false
  } else {
	if vs.servers[args.Me].lastpingedtime.IsZero() == true {
		vs.serverscount += 1
	}
	if vs.currentview.Backup == args.Me {
		if args.Viewnum == vs.currentview.Viewnum {
			vs.backupAcked = true
		}
		if args.Viewnum == 0  &&
			vs.newview.Backup != args.Me &&
				vs.backupAcked != false &&
				isBackupNotSet == false {
			//backup restarted
			vs.currentview.Backup = ""
			vs.newview.Primary = vs.currentview.Primary
			vs.newview.Backup = args.Me
			hasViewChanged = true
			vs.backupAcked = false
		}
	}
	if vs.currentview.Primary == args.Me {
	   if args.Viewnum == vs.currentview.Viewnum {
	    vs.lastackedview = vs.currentview.Viewnum
	    if vs.currentview.Viewnum < vs.newview.Viewnum {
	      //primary is operating in current view safe to change new view
	      vs.currentview = vs.newview
	      vs.newview = View {0, "", ""}
	     }
	    } else {
		if vs.primarychanged == false &&
			args.Viewnum == 0 &&
				vs.lastackedview == vs.currentview.Viewnum &&
					vs.backupAcked == true {
		 //primary restarted
		  vs.currentview.Primary = vs.currentview.Backup
		  vs.currentview.Backup = ""
		  vs.currentview.Viewnum += 1
		  vs.newview = View {0, "", ""}
                  vs.primarychanged = true
		  vs.backupAcked = false
		  } else {
		    vs.primarychanged = false
		  }
	     }
	}
  }
  vs.servers[args.Me] = ServerPingInfo { args.PingTime, args.Viewnum }
  if hasViewChanged == true {
	vs.newview.Viewnum = vs.currentview.Viewnum + 1
  }
  reply.View = vs.currentview
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  if vs.currentview.Primary == "" {
	return errors.New("View not set")
  }
  reply.View = vs.currentview
  return nil
}


func isAlive(t time.Time) bool {
	//added random hack because time isnt exact due to lock
	if time.Since(t).Nanoseconds()/1000000 - 10 > PingInterval.Nanoseconds()/1000000 {
		return false
	} else {
		return true
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  //isPrimaryNotSet := vs.currentview.Primary == "" && vs.newview.Primary == ""
  isBackupNotSet := vs.currentview.Backup == "" && vs.newview.Backup == ""

  if vs.currentview.Primary != "" {
  //check if primary is alive
   if isAlive(vs.servers[vs.currentview.Primary].lastpingedtime) == false {
     if vs.lastackedview == vs.currentview.Viewnum && 
		vs.backupAcked == true {
	delete(vs.servers, vs.currentview.Primary)
	vs.serverscount -= 1
	vs.currentview.Primary = vs.currentview.Backup
	vs.currentview.Viewnum +=  1
	//isPrimaryNotSet = vs.currentview.Backup == ""
	vs.currentview.Backup = ""
	vs.primarychanged = true
	vs.backupAcked = false
       }
    }
  }
  if vs.currentview.Backup != "" {
  //check if backup is alive
   if isAlive(vs.servers[vs.currentview.Backup].lastpingedtime) == false {
	 if vs.lastackedview == vs.currentview.Viewnum {
	  delete(vs.servers, vs.currentview.Backup)
	  vs.serverscount -= 1
	  vs.currentview.Backup = ""
	  isBackupNotSet = true
	}
     }
   }

  // check if servers are alive
  for server, info := range vs.servers {
	if server == vs.currentview.Primary ||
		server == vs.currentview.Backup {
		continue
	}
	if isAlive(info.lastpingedtime) == false  {
	     delete(vs.servers, server)
        } else {
		if isBackupNotSet == true {
		 if vs.lastackedview == vs.currentview.Viewnum {
		   vs.newview.Primary = vs.currentview.Primary
		   vs.newview.Backup = server
		   vs.newview.Viewnum = vs.currentview.Viewnum + 1
		   isBackupNotSet = false
		  }
		}
	}
   }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.currentview = View {0, "", "" }
  vs.newview = View {0, "", "" }
  vs.lastackedview = 0
  vs.servers = map[string]ServerPingInfo {}
  vs.viewuninitialized = true
  vs.viewbackupuninitialized = true
  vs.backupAcked = false
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
