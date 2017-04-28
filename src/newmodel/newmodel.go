package newmodel

import (
	"container/list"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"net/rpc"
	"newmodelproto"
	"state"
	"strings"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10
const HEART_BEAT = 500000000

//const MAX_BATCH        = 5000
const MAX_BATCH = 1

type Replica struct {
	*genericsmr.Replica
	prepareChan        chan fastrpc.Serializable
	acceptChan         chan fastrpc.Serializable
	commitChan         chan fastrpc.Serializable
	commitShortChan    chan fastrpc.Serializable
	prepareReplyChan   chan fastrpc.Serializable
	acceptReplyChan    chan fastrpc.Serializable
	prepareCLChan      chan fastrpc.Serializable
	acceptCLChan       chan fastrpc.Serializable
	prepareCLReplyChan chan fastrpc.Serializable
	acceptCLReplyChan  chan fastrpc.Serializable
	requestCLChan      chan fastrpc.Serializable
	requestReplyChan   chan fastrpc.Serializable
	commitCLChan       chan fastrpc.Serializable
	askForVoteChan     chan fastrpc.Serializable
	voteChan           chan fastrpc.Serializable
	winningChan        chan fastrpc.Serializable
	prepareRPC         uint8
	acceptRPC          uint8
	commitRPC          uint8
	commitShortRPC     uint8
	prepareReplyRPC    uint8
	acceptReplyRPC     uint8
	prepareCLRPC       uint8
	acceptCLRPC        uint8
	prepareCLReplyRPC  uint8
	acceptCLReplyRPC   uint8
	requestCLRPC       uint8
	requestReplyRPC    uint8
	commitCLRPC        uint8
	askForVoteRPC      uint8
	voteRPC            uint8
	winningRPC         uint8
	isLeader           bool
	privateInstance    [][]*Instance
	readSpace          []*Read
	nextAssigned       int32
	nextUsed           int32
	nextRead           int32
	assign             []*Assignation
	clockChan          chan bool
	flushClockChan     chan bool
	defaultBallot      int32 //default to 0 for simplicity
	Shutdown           bool
	executedUpTo       int32
	privateExectued    []int32
	acceptedUpTo       []int32
	acceptedCL         []int32
	acceptedUpToCL     int32
	allocated          int32
	semiCommitChan     chan int32
	increment          chan bool
	currentTerm        int32
	currentLeader      int32
	gapChan            chan bool
	numberLatency      []time.Time
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	READY
	SEMICOMMITTED
	COMMITTED
)

type Instance struct {
	cmds         []state.Command
	ballot       int32
	status       InstanceStatus
	clb          *CLeaderBookkeeping
	pendingReads *list.List
}

type CLeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
}

type Read struct {
	lastInstance int32
	proposal     *genericsmr.Propose
}

type Assignation struct {
	commandLeader int32
	ballot        int32
	assignOKs     int
	status        InstanceStatus
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, beacon bool, durable bool, slow bool) *Replica {
	//TODO: Why is the buffer for prepareReplyChan larger?
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		false,
		make([][]*Instance, len(peerAddrList)),
		make([]*Read, 15*1024*1024),
		0, 0, 0,
		make([]*Assignation, 15*1024*1024),
		make(chan bool, 1),
		make(chan bool, 1),
		0, false, -1,
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		-1, -1,
		make(chan int32, genericsmr.CHAN_BUFFER_SIZE),
		make(chan bool, genericsmr.CHAN_BUFFER_SIZE),
		0, 0,
		make(chan bool, 10000),
		make([]time.Time, 5*1024*1024)}

	r.Beacon = beacon
	r.Durable = durable
	r.Slow = slow
	for i := 0; i < r.N; i++ {
		r.privateInstance[i] = make([]*Instance, 5*1024*1024)
		r.privateExectued[i] = -1
		r.acceptedUpTo[i] = -1
		r.acceptedCL[i] = -1
	}

	r.prepareRPC = r.RegisterRPC(new(newmodelproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(newmodelproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(newmodelproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(newmodelproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(newmodelproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(newmodelproto.AcceptReply), r.acceptReplyChan)
	r.prepareCLRPC = r.RegisterRPC(new(newmodelproto.PrepareCL), r.prepareCLChan)
	r.acceptCLRPC = r.RegisterRPC(new(newmodelproto.AcceptCL), r.acceptCLChan)
	r.prepareCLReplyRPC = r.RegisterRPC(new(newmodelproto.PrepareCLReply), r.prepareCLReplyChan)
	r.acceptCLReplyRPC = r.RegisterRPC(new(newmodelproto.AcceptCLReply), r.acceptCLReplyChan)
	r.requestCLRPC = r.RegisterRPC(new(newmodelproto.RequestCL), r.requestCLChan)
	r.requestReplyRPC = r.RegisterRPC(new(newmodelproto.RequestReply), r.requestReplyChan)
	r.commitCLRPC = r.RegisterRPC(new(newmodelproto.CommitCL), r.commitCLChan)
	r.askForVoteRPC = r.RegisterRPC(new(newmodelproto.AskForVote), r.askForVoteChan)
	r.voteRPC = r.RegisterRPC(new(newmodelproto.Vote), r.voteChan)
	r.winningRPC = r.RegisterRPC(new(newmodelproto.Winning), r.winningChan)

	go r.run()
	go r.runCL()

	return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}

	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.isLeader = true
	return nil
}

var mrepp genericsmr.Message

func (r *Replica) replyPrepare(replicaId int32, reply *newmodelproto.PrepareReply) {
	//r.SendMsg(replicaId, r.prepareReplyRPC, reply)
	mrepp.PeerId = replicaId
	mrepp.Code = r.prepareReplyRPC
	mrepp.Msg = reply
	r.MessageChan <- mrepp
}

var mrepa genericsmr.Message

func (r *Replica) replyAccept(replicaId int32, reply *newmodelproto.AcceptReply) {
	//r.SendMsg(replicaId, r.acceptReplyRPC, reply)
	mrepa.PeerId = replicaId
	mrepa.Code = r.acceptReplyRPC
	mrepa.Msg = reply
	r.MessageChan <- mrepa
}

func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(1000 * 1000 * 150)
		r.clockChan <- true
	}
}

func (r *Replica) tpClock() {
	tp := int32(0)
	tpAssignation := int32(0)
	for !r.Shutdown {
		time.Sleep(1000 * 1000 * 1000)
		if r.isLeader {
			fmt.Printf("Throughput of assignation is %v\n", r.acceptedUpToCL-tpAssignation)
			tpAssignation = r.acceptedUpToCL
		}
		fmt.Printf("Throughput is %v\n", r.nextUsed-tp)
		tp = r.nextUsed
	}
}

func (r *Replica) monitorLeader(pingChan chan bool) {
	heartBeat := time.Duration(r.Id * HEART_BEAT)
	addrSplit := strings.Split(r.PeerAddrList[r.currentLeader], ":")
	leader, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:8070", addrSplit[0]))
	if err != nil {
		log.Fatal("Error when connecting to the leader")
	}

	for !r.Shutdown {
		time.Sleep(heartBeat)
		pingErr := leader.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
		if pingErr != nil {
			log.Println("The leader is suspected of having failed, trying to take over")
			pingChan <- true
			break
		}
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	log.Println(r.PreferredPeerOrder)
}

/* Main event processing loop */
func (r *Replica) run() {
	r.ConnectToPeers()
	dlog.Println("Waiting for client connections")
	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
	}

	if r.Id == r.currentLeader {
		r.isLeader = true
	}

	pingChan := make(chan bool, 1)

	if r.Id == r.currentLeader+1 {
		go r.monitorLeader(pingChan) //TODO
	}

	go r.slowClock()

	//if MAX_BATCH > 100 {
	//	go r.clock()
	//}

	if r.isLeader {
		go r.handleRequestCL()
		//go r.tpClock()
	}

	go r.replyToClient()

	if r.Beacon {
		go r.stopAdapting()
	}
	onOffProposeChan := r.ProposeChan
	flushMessage := genericsmr.Message{-1, 0, nil, false}

	//Initialization is omitted by setting default ballot to 0

	for !r.Shutdown {

		select {

		case propose := <-onOffProposeChan:
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			r.handlePropose(propose)
			//onOffProposeChan = nil
			break

		case prepareS := <-r.prepareChan:
			//This is type assertion
			prepare := prepareS.(*newmodelproto.Prepare)
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*newmodelproto.Accept)
			dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*newmodelproto.Commit)
			dlog.Printf("Receive Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break

		case commitShortS := <-r.commitShortChan:
			commiShort := commitShortS.(*newmodelproto.CommitShort)
			dlog.Printf("Receive CommitShort from replica %d, for instance %d\n", commiShort.LeaderId, commiShort.Instance)
			r.handleCommitShort(commiShort)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*newmodelproto.PrepareReply)
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*newmodelproto.AcceptReply)
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break

		case beacon := <-r.BeaconChan:
			dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			r.ReplyBeacon(beacon)
			break

		case <-r.clockChan:
			if r.Beacon {
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			r.MessageChan <- flushMessage
			break

		case <-pingChan:
			r.handleLeaderFailure()
			break

		case askForVoteS := <-r.askForVoteChan:
			askForVote := askForVoteS.(*newmodelproto.AskForVote)
			dlog.Printf("Receive AskForVote from replica %d, for term %d\n", askForVote.Candidate, askForVote.Term)
			r.handleAskForVote(askForVote)
			break
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
	return r.makeUniqueBallot((ballot >> 4) + 1)
}

var mnp genericsmr.Message

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &newmodelproto.Prepare{r.Id, instance, ballot, ti}
	mnp.Code = r.prepareRPC
	mnp.Msg = args

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}

	q := 0
	for sent := 0; sent < n; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		//r.SendMsg(q, r.prepareRPC, args)
		mnp.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mnp
	}
}

var mna genericsmr.Message

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Printf("Replica %d, instance %d\n", r.Id, instance)
			log.Printf("Accept bcast for instance failed:", err)
		}
	}()
	var na newmodelproto.Accept
	na.LeaderId = r.Id
	na.Instance = instance
	na.Ballot = ballot
	na.Command = command
	//args := &na
	mna.Code = r.acceptRPC
	mna.Msg = &na
	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := 0

	sentToLeader := false
	for sent := 0; sent < n; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if r.PreferredPeerOrder[q] == r.currentLeader {
			r.numberLatency[instance] = time.Now()
			sentToLeader = true
		}
		sent++
		//r.SendMsg(q, r.acceptRPC, args)
		mna.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mna
	}
	if !sentToLeader {
		if r.isLeader {
			reqLocal := newmodelproto.RequestCL{r.Id, FALSE, int64(command[0].K), instance}
			r.requestCLChan <- &reqLocal
		} else {
			r.SendRequest(false, int64(command[0].K), instance)
		}
	}
}

var mnc genericsmr.Message
var mncs genericsmr.Message

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	var nc newmodelproto.Commit
	var ncs newmodelproto.CommitShort

	//CommitShort cannot guarantee safety. But... whatever..
	nc.LeaderId = r.Id
	nc.Instance = instance
	nc.Ballot = ballot
	nc.Command = command
	//args := &nc
	mnc.Code = r.commitRPC
	mnc.Msg = &nc

	ncs.LeaderId = r.Id
	ncs.Instance = instance
	ncs.Count = int32(len(command))
	ncs.Ballot = ballot
	//argsShort := &ncs
	mncs.Code = r.commitShortRPC
	mncs.Msg = &ncs

	//n := r.N - 1
	//if r.Thrifty {
	//	n = r.N >> 1
	//}
	//q := 0
	sent := 0

	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if r.Thrifty && sent >= r.N/2 {
			//r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, args)
			mnc.PeerId = r.PreferredPeerOrder[q]
			r.MessageChan <- mnc
		} else {
			//r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
			mncs.PeerId = r.PreferredPeerOrder[q]
			r.MessageChan <- mncs
			sent++
		}
	}
	//for ; sent < n; q++ {
	//	if !r.Alive[r.PreferredPeerOrder[q]] {
	//		continue
	//	}
	//	sent++
	//	//r.SendMsg(q, r.commitShortRPC, argsShort)
	//	mncs.PeerId = r.PreferredPeerOrder[q]
	//	r.MessageChan <- mncs
	//}
	//if r.Thrifty && r.PreferredPeerOrder[q] != r.Id {
	//	for sent < r.N-1 {
	//		if !r.Alive[r.PreferredPeerOrder[q]] {
	//			q++
	//			continue
	//		}
	//		if r.PreferredPeerOrder[q] == r.Id {
	//			break
	//		}
	//		sent++
	//		//r.SendMsg(q, r.commitRPC, args)
	//		mnc.PeerId = r.PreferredPeerOrder[q]
	//		r.MessageChan <- mnc
	//		q++
	//	}
	//}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	if propose.Command.Op == state.GET {
		dlog.Println("Requesting for a read operation")
		r.readSpace[r.nextRead] = &Read{-1, propose}
		if r.isLeader {
			r.requestCLChan <- &newmodelproto.RequestCL{r.Id, TRUE, int64(propose.Command.K), r.nextRead}
			r.nextRead++
			return
		}
		r.SendRequest(true, int64(propose.Command.K), r.nextRead)
		r.nextRead++
		return
	}
	instNo := r.nextUsed
	r.nextUsed++
	dlog.Printf("Choosing private instance number %d\n", instNo)
	batchSize := len(r.ProposeChan) + 1
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	dlog.Printf("Batched %d\n", batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	inst := r.privateInstance[r.Id][instNo]
	if inst != nil {
		inst.cmds = cmds
		inst.ballot = r.makeUniqueBallot(0)
		inst.status = ACCEPTED
		inst.clb.clientProposals = proposals
	} else {
		r.privateInstance[r.Id][instNo] = &Instance{
			cmds,
			r.makeUniqueBallot(0),
			ACCEPTED,
			&CLeaderBookkeeping{proposals, 0, 0, 0, 0},
			nil}
	}
	r.acceptedUpTo[r.Id] = instNo
	r.bcastAccept(instNo, r.privateInstance[r.Id][instNo].ballot, cmds)
	dlog.Printf("Accept Phase for instance %d\n", instNo)
	if r.isLeader {
	}
}

func (r *Replica) handlePrepare(prepare *newmodelproto.Prepare) {
	inst := r.privateInstance[prepare.LeaderId][prepare.Instance]
	var preply *newmodelproto.PrepareReply

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &newmodelproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	} else {
		ok := TRUE
		if inst.ballot > prepare.Ballot {
			ok = FALSE
		}
		preply = &newmodelproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}
	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *newmodelproto.Accept) {
	if r.isLeader {
		r.requestCLChan <- &newmodelproto.RequestCL{accept.LeaderId, FALSE, int64(accept.Command[0].K), accept.Instance}
	}
	inst := r.privateInstance[accept.LeaderId][accept.Instance]
	var areply *newmodelproto.AcceptReply

	if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &newmodelproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			r.privateInstance[accept.LeaderId][accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil,
				nil}
			areply = &newmodelproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &newmodelproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &newmodelproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
		if inst.clb != nil && inst.clb.clientProposals != nil {
			for i := 0; i < len(inst.clb.clientProposals); i++ {
				r.ProposeChan <- inst.clb.clientProposals[i]
			}
			inst.clb.clientProposals = nil
		}
	} else {
		inst.cmds = accept.Command
		if inst.status != COMMITTED && inst.status != SEMICOMMITTED {
			inst.status = ACCEPTED
		}
		areply = &newmodelproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
	}

	if areply.OK == TRUE {
		if accept.Instance > r.acceptedUpTo[accept.LeaderId] {
			r.acceptedUpTo[accept.LeaderId] = accept.Instance
		}
		r.recordInstanceMetadata(inst)
		r.recordCommands(accept.Command)
		r.sync()
	}

	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *newmodelproto.Commit) {
	inst := r.privateInstance[commit.LeaderId][commit.Instance]

	dlog.Printf("Committing instance %d of replica %d\n", commit.Instance, commit.LeaderId)

	if inst == nil {
		r.privateInstance[commit.LeaderId][commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			SEMICOMMITTED,
			nil,
			nil}
	} else {
		inst.cmds = commit.Command
		inst.status = SEMICOMMITTED
		inst.ballot = commit.Ballot
		if inst.clb != nil && inst.clb.clientProposals != nil {
			for i := 0; i < len(inst.clb.clientProposals); i++ {
				r.ProposeChan <- inst.clb.clientProposals[i]
			}
			inst.clb.clientProposals = nil
		}
	}
	if r.isLeader {
		//TODO
	}

	if commit.Instance > r.acceptedUpTo[commit.LeaderId] {
		r.acceptedUpTo[commit.LeaderId] = commit.Instance
	}
	//r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.privateInstance[commit.LeaderId][commit.Instance])
	//r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *newmodelproto.CommitShort) {
	inst := r.privateInstance[commit.LeaderId][commit.Instance]

	dlog.Printf("Committing instance %d of replica %d\n", commit.Instance, commit.LeaderId)

	if inst == nil {
		r.privateInstance[commit.LeaderId][commit.Instance] = &Instance{nil,
			commit.Ballot,
			SEMICOMMITTED,
			nil,
			nil}
	} else {
		inst.status = SEMICOMMITTED
		inst.ballot = commit.Ballot
		if inst.clb != nil && inst.clb.clientProposals != nil {
			for i := 0; i < len(inst.clb.clientProposals); i++ {
				r.ProposeChan <- inst.clb.clientProposals[i]
			}
			inst.clb.clientProposals = nil
		}
	}
	if r.isLeader {
		//TODO
	}
	if commit.Instance > r.acceptedUpTo[commit.LeaderId] {
		r.acceptedUpTo[commit.LeaderId] = commit.Instance
	}
	//r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.privateInstance[commit.LeaderId][commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *newmodelproto.PrepareReply) {
	inst := r.privateInstance[r.Id][preply.Instance]

	if inst.status != PREPARING {
		// TODO: schould replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.clb.prepareOKs++

		if preply.Ballot > inst.clb.maxRecvBallot {
			inst.cmds = preply.Command
			inst.clb.maxRecvBallot = preply.Ballot
			if inst.clb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.clb.clientProposals); i++ {
					r.ProposeChan <- inst.clb.clientProposals[i]
				}
				inst.clb.clientProposals = nil
			}
		}

		if inst.clb.prepareOKs+1 > r.N>>1 {
			inst.status = PREPARED
			inst.clb.nacks = 0
			//if inst.ballot > r.defaultBallot {
			//	//TODO: why?
			//	r.defaultBallot = inst.ballot
			//}
			r.recordInstanceMetadata(r.privateInstance[r.Id][preply.Instance])
			r.sync()
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		// TODO: there is probably another active leader
		inst.clb.nacks++
		if preply.Ballot > inst.clb.maxRecvBallot {
			inst.clb.maxRecvBallot = preply.Ballot
		}
		if inst.clb.nacks >= r.N>>1 {
			if inst.clb != nil {
				inst.ballot = r.makeBallotLargerThan(inst.clb.maxRecvBallot)
				r.bcastPrepare(preply.Instance, inst.ballot, false)
			}
		}
	}
}

func (r *Replica) handleAcceptReply(areply *newmodelproto.AcceptReply) {
	inst := r.privateInstance[r.Id][areply.Instance]

	if inst.status != PREPARED && inst.status != ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		inst.clb.acceptOKs++
		if inst.clb.acceptOKs+1 > r.N>>1 {
			for i := 0; i < len(inst.cmds); i++ {
				if inst.cmds[i].Op == state.PUT {
					r.semiCommitChan <- areply.Instance
				}
			}
			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds)
			//inst.status = SEMICOMMITTED

			r.recordInstanceMetadata(r.privateInstance[r.Id][areply.Instance])
			r.sync() //is this necessary?

			if r.isLeader {
				//TODO
			}
		}
	} else {
		// TODO: there is probably another active leader
		inst.clb.nacks++
		if areply.Ballot > inst.clb.maxRecvBallot {
			inst.clb.maxRecvBallot = areply.Ballot
		}
		if inst.clb.nacks >= r.N>>1 {
			if inst.clb != nil {
				inst.ballot = r.makeBallotLargerThan(inst.clb.maxRecvBallot)
				r.bcastPrepare(areply.Instance, inst.ballot, false)
			}
		}
	}
}

/* functions for communication with clients */

var writeReply genericsmr.ReplyMessage

func (r *Replica) replyToClient() {
	for !r.Shutdown {
		select {
		case commitM := <-r.semiCommitChan:
			if commitM <= r.allocated {
				inst := r.privateInstance[r.Id][commitM]
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					inst.clb.clientProposals[0].CommandId,
					state.NIL,
					inst.clb.clientProposals[0].Timestamp}
				//r.ReplyProposeTS(propreply, inst.clb.clientProposals[i].Reply)
				writeReply.Reply = propreply
				writeReply.Writer = inst.clb.clientProposals[0].Reply
				r.ReplyMessageChan <- writeReply
				dlog.Println("Replying write request")
				inst.status = SEMICOMMITTED
			} else {
				r.privateInstance[r.Id][commitM].status = READY
			}
			break

		case <-r.increment:
			r.allocated++
			fmt.Printf("Latency of the %dth number is %v\n", r.allocated, time.Now().Sub(r.numberLatency[r.allocated]).Seconds()*1000)
			if r.privateInstance[r.Id][r.allocated] != nil && r.privateInstance[r.Id][r.allocated].status == READY {
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					r.privateInstance[r.Id][r.allocated].clb.clientProposals[0].CommandId,
					state.NIL,
					r.privateInstance[r.Id][r.allocated].clb.clientProposals[0].Timestamp}
				writeReply.Reply = propreply
				writeReply.Writer = r.privateInstance[r.Id][r.allocated].clb.clientProposals[0].Reply
				r.ReplyMessageChan <- writeReply
				r.privateInstance[r.Id][r.allocated].status = SEMICOMMITTED
			}
			break
		}
	}
}

var readReply genericsmr.ReplyMessage

func (r *Replica) handleRequestReply(reqreply *newmodelproto.RequestReply) {
	if r.readSpace[reqreply.ReadSeq] == nil {
		return
	} else if r.privateExectued[reqreply.LeaderId] < reqreply.Instance {
		dlog.Printf("Read %d is pending, instance %d of replica %d\n", reqreply.ReadSeq, reqreply.Instance, reqreply.LeaderId)
		if r.privateInstance[reqreply.LeaderId][reqreply.Instance] == nil {
			r.privateInstance[reqreply.LeaderId][reqreply.Instance] = &Instance{
				nil,
				r.defaultBallot,
				PREPARING,
				nil,
				nil}
		}
		inst := r.privateInstance[reqreply.LeaderId][reqreply.Instance]
		if inst.pendingReads == nil {
			inst.pendingReads = list.New()
		}
		inst.pendingReads.PushBack(reqreply.ReadSeq)
	} else {
		dlog.Printf("Read %d can now be done\n", reqreply.ReadSeq)
		read := r.readSpace[reqreply.ReadSeq]
		val := read.proposal.Command.Execute(r.State)
		propreply := &genericsmrproto.ProposeReplyTS{
			TRUE,
			read.proposal.CommandId,
			val,
			read.proposal.Timestamp}
		//r.ReplyProposeTS(propreply, read.proposal.Reply)
		readReply.Reply = propreply
		readReply.Writer = read.proposal.Reply
		dlog.Println("Replying read request")
		r.ReplyMessageChan <- readReply
	}
}

func (r *Replica) executeCommands() {
	var val state.Value
	executed := true
	for !r.Shutdown {
		select {
		case requestReplyS := <-r.requestReplyChan:
			requestReply := requestReplyS.(*newmodelproto.RequestReply)
			dlog.Printf("Received RequestReply for read %d\n", requestReply.ReadSeq)
			r.handleRequestReply(requestReply)
			executed = true

		default:
			if r.assign[r.executedUpTo+1] != nil &&
				r.assign[r.executedUpTo+1].status == COMMITTED &&
				r.privateInstance[r.assign[r.executedUpTo+1].commandLeader][r.privateExectued[r.assign[r.executedUpTo+1].commandLeader]+1] != nil &&
				r.privateInstance[r.assign[r.executedUpTo+1].commandLeader][r.privateExectued[r.assign[r.executedUpTo+1].commandLeader]+1].status == SEMICOMMITTED {
				executed = true
				inst := r.privateInstance[r.assign[r.executedUpTo+1].commandLeader][r.privateExectued[r.assign[r.executedUpTo+1].commandLeader]+1]
				inst.status = COMMITTED

				inst.cmds[0].Execute(r.State)
				r.executedUpTo++
				r.privateExectued[r.assign[r.executedUpTo].commandLeader]++
				dlog.Printf("Successfully executed command %d\n", r.executedUpTo)

				for inst.pendingReads != nil && inst.pendingReads.Len() > 0 {
					dlog.Println("Handling pending reads")
					readseq := inst.pendingReads.Remove(inst.pendingReads.Front()).(int32)
					read := r.readSpace[readseq]
					val = read.proposal.Command.Execute(r.State)
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						read.proposal.CommandId,
						val,
						read.proposal.Timestamp}
					//r.ReplyProposeTS(propreply, read.proposal.Reply)
					readReply.Reply = propreply
					readReply.Writer = read.proposal.Reply
					dlog.Println("Replying read request")
					r.ReplyMessageChan <- readReply
				}
			} else {
				executed = false
			}
		}
		if !executed {
			time.Sleep(1000 * 1000)
		}
	}
}
