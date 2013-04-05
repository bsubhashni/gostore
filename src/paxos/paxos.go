package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"


type Proposal struct{
	Seq int
	Value interface{}
	Proposalnumber int
}
type AcceptReply struct{
	Decision bool
	Highestproposal int
}

type Instance struct{
	value interface{}
	highestaccept int
	highestproposal int
	decided bool
}
type Sequence struct {
	id int
	value interface{}
}
type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  majority int
  proposalhint int
  receiver chan Sequence
  history map[int]Instance
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  } 
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  _, ok := px.history[seq]
  if ok == false {
	//start new instance
	fmt.Printf("test start \n ")
	newsequence := Sequence {seq, v}
	go px.propose(newsequence)
  }
  return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  return false, nil
}
//paxos protocol actors
func (px *Paxos) propose(seq Sequence){
   px.mu.Lock()
   defer px.mu.Unlock()
   divider := 1
   proposalnumber := px.proposalhint + (px.proposalhint % (divider + px.me))
   if px.dead == true {
	return
   }
   //start new agreement - proposal phase
   px.majority = len(px.peers) + 1
   px.history[seq.id] = Instance {nil, 0 , 0, false}
   newproposal  :=  Proposal { seq.id, seq.value, proposalnumber }
   var reply AcceptReply
   acceptcounter := 0
   for _,peer:= range px.peers {
	fmt.Printf("calling \n")
	ok := call(peer, "Paxos.Accept", &newproposal, &reply)
	if ok == true {
		if reply.Decision == true {
		    acceptcounter += 1
		} else {
		    if px.proposalhint < reply.Highestproposal {
			px.proposalhint = reply.Highestproposal
		    }
		}
	} else {
	   //delete(px.peers, peer)
	}
   }
  px.majority = len(px.peers)/2 +1
  decidecounter := 0
  if acceptcounter >= px.majority {
	//start the decide phase
  //decidecounter := 0
  var decidereply AcceptReply
   for _, peer := range px.peers {
	ok := call(peer, "Paxos.Decide", &newproposal, &decidereply)
	if ok == true {
	    if decidereply.Decision == true  {
		decidecounter += 1
	    } else {
		if px.proposalhint < decidereply.Highestproposal {
			px.proposalhint = decidereply.Highestproposal
		}
	    }
	} else {
	    //server not responding
	    //delete(px.peers, peer)
	}
    }
  }
 px.majority = len(px.peers)/2 +1
 decidedcounter := 0
 if decidecounter >= px.majority {
	var v interface{}
	for _, peer := range px.peers {
		ok := call(peer, "Paxos.Decided", &newproposal, &v)
		if ok == false {
			//delete(px.peers, peer)
		} else {
			decidedcounter++
		}
	}
 }
 return
}
func (px *Paxos) Accept(args *Proposal, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  instance ,ok := px.history[args.Seq]
  if ok == true {
	if instance.highestproposal >= args.Proposalnumber {
		reply.Decision = false
		reply.Highestproposal = instance.highestproposal
		return nil
	}
   } else {
	px.history[args.Seq] = Instance {nil, 0, 0, false}
   }
   reply.Decision = true
   reply.Highestproposal = args.Proposalnumber
   instance.highestproposal = args.Proposalnumber
   return nil
}
func (px *Paxos) Decide(args *Proposal, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  instance, _ := px.history[args.Seq]
  if args.Proposalnumber >= instance.highestproposal {
	instance.highestaccept = args.Proposalnumber
	instance.value = args.Value
	reply.Decision = true
	reply.Highestproposal = args.Proposalnumber
  } else {
	reply.Decision = false
	reply.Highestproposal = instance.highestproposal
  }
  return nil
}
func (px *Paxos) Decided(args *Proposal, v *interface{}) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  instance,_ := px.history[args.Seq]
  instance.value = args.Value
  instance.decided = true
  return nil
}
//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  // Your initialization code here.
  px.majority = len(px.peers)/2 + 1
  px.proposalhint = 1
  px.receiver = make(chan Sequence)
  px.history =  map[int]Instance {}

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
