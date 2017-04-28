package newmodel

import (
	"dlog"
	"genericsmr"
	//"genericsmrproto"
	"log"
	"newmodelproto"
	//"state"
	"time"
)

type History struct {
	LeaderId int32
	Instance int32
}

var mrpcl genericsmr.Message

func (r *Replica) replyPrepareCL(replicaId int32, reply *newmodelproto.PrepareCLReply) {
	//r.SendMsg(replicaId, r.prepareCLReplyRPC, reply)
	mrpcl.PeerId = replicaId
	mrpcl.Code = r.prepareCLReplyRPC
	mrpcl.Msg = reply
	r.MessageChan <- mrpcl
}

var mracl genericsmr.Message

func (r *Replica) replyAcceptCL(replicaId int32, reply *newmodelproto.AcceptCLReply) {
	//r.SendMsg(replicaId, r.acceptCLReplyRPC, reply)
	mracl.PeerId = replicaId
	mracl.Code = r.acceptCLReplyRPC
	mracl.Msg = reply
	mracl.NoFlush = true
	r.MessageChan <- mracl
}

var mreqrep genericsmr.Message

func (r *Replica) replyRequest(seq int32, instance int32, clid int32, leader int32) {
	var reqrep newmodelproto.RequestReply
	reqrep.ReadSeq = seq
	reqrep.Instance = instance
	reqrep.LeaderId = clid
	if leader == r.Id {
		r.requestReplyChan <- &reqrep
		return
	}
	mreqrep.PeerId = leader
	mreqrep.Code = r.requestReplyRPC
	mreqrep.Msg = &reqrep
	r.MessageChan <- mreqrep
}

var mnpcl genericsmr.Message

func (r *Replica) bcastPrepareCL(instance int32, ballot int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("PrepareCL bcast failed:", err)
		}
	}()
	args := &newmodelproto.PrepareCL{r.Id, instance, ballot}
	mnpcl.Code = r.prepareCLRPC
	mnpcl.Msg = args

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := 0

	for sent := 0; sent < n; q++ {
		if r.PreferredPeerOrder[q] == r.Id {
			break
		}
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		mnpcl.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mnpcl
	}
}

var mnacl genericsmr.Message

func (r *Replica) bcastAcceptCL(instance int32, ballot int32, clid int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Printf("Replica %d, instance %d\n", r.Id, instance)
			log.Println("AcceptCL bcast failed:", err)
		}
	}()
	var nacl newmodelproto.AcceptCL
	nacl.LeaderId = r.Id
	nacl.Instance = instance
	nacl.Ballot = ballot
	nacl.CLId = clid
	//args := &nacl
	mnacl.Code = r.acceptCLRPC
	mnacl.Msg = &nacl
	mnacl.NoFlush = true

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}

	sent := 0

	//Ensure that the command leader will receive this message
	if clid != r.Id {
		//r.SendMsg(clid, r.acceptCLRPC, args)
		mnacl.PeerId = clid
		r.MessageChan <- mnacl
		sent++
	}

	for q := 0; sent < n; q++ {
		if r.PreferredPeerOrder[q] == clid {
			continue
		}
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		//r.SendMsg(q, r.acceptCLRPC, args)
		mnacl.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mnacl
	}
}

var mncl genericsmr.Message

func (r *Replica) bcastCommitCL(instance int32, clid int32, ballot int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Printf("Replica %d, instance %d\n", r.Id, instance)
			log.Println("CommitCL bcast failed:", err)
		}
	}()
	var ncl newmodelproto.CommitCL
	ncl.CLId = clid
	ncl.Instance = instance
	ncl.Ballot = ballot
	mncl.Code = r.commitCLRPC
	mncl.Msg = &ncl
	mncl.NoFlush = true
	n := r.N - 1
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		mncl.PeerId = q
		r.MessageChan <- mncl
	}
}

var mreq genericsmr.Message

func (r *Replica) SendRequest(isRead bool, obj int64, instance int32) {
	//args := new(newmodelproto.RequestCL)
	args := newmodelproto.RequestCL{r.Id, FALSE, obj, instance}
	if isRead {
		args.IsRead = TRUE
	} else {
		r.numberLatency[instance] = time.Now()
	}
	//r.SendMsg(0, r.requestCLRPC, &args)
	mreq.PeerId = r.currentLeader
	mreq.Code = r.requestCLRPC
	mreq.Msg = &args
	r.MessageChan <- mreq
}

func (r *Replica) handleRequestCL() {
	requestTable := make(map[int64]*History, 200000)

	for !r.Shutdown {
		requestS := <-r.requestCLChan
		request := requestS.(*newmodelproto.RequestCL)
		if request.IsRead == FALSE {
			instNo := r.nextAssigned
			r.nextAssigned++
			r.assign[instNo] = &Assignation{request.LeaderId, r.makeUniqueBallot(0), 0, ACCEPTED}
			r.acceptedCL[request.LeaderId]++
			if instNo > r.acceptedUpToCL {
				r.acceptedUpToCL = instNo
			}
			dlog.Printf("Assign a new number for replica %d\n", request.LeaderId)
			if request.LeaderId == r.Id {
				r.increment <- true
			}
			r.bcastAcceptCL(instNo, r.makeUniqueBallot(0), request.LeaderId)

			if _, found := requestTable[request.Object]; found {
				requestTable[request.Object].LeaderId = request.LeaderId
				requestTable[request.Object].Instance = request.Instance
			} else {
				requestTable[request.Object] = &History{request.LeaderId, request.Instance}
			}
		} else {
			dlog.Printf("Received read request from replica %d\n", request.LeaderId)
			if history, found := requestTable[request.Object]; found {
				r.replyRequest(request.Instance, history.Instance, history.LeaderId, request.LeaderId)
			} else {
				r.replyRequest(request.Instance, -2, 0, request.LeaderId)
			}
		}
	}
}

func (r *Replica) handlePrepareCL(prepareCL *newmodelproto.PrepareCL) {
	var pclreply *newmodelproto.PrepareCLReply
	pclreply = &newmodelproto.PrepareCLReply{prepareCL.Instance, TRUE, r.defaultBallot, -1}
	if r.assign[prepareCL.Instance] != nil {
		pclreply.CLId = r.assign[prepareCL.Instance].commandLeader
		pclreply.Ballot = r.assign[prepareCL.Instance].ballot
	}
	r.replyPrepareCL(prepareCL.LeaderId, pclreply)
}

func (r *Replica) handleAcceptCL(acceptCL *newmodelproto.AcceptCL) {
	if acceptCL.CLId == r.Id {
		r.increment <- true
	}
	var aclrep newmodelproto.AcceptCLReply
	if r.assign[acceptCL.Instance] == nil {
		r.assign[acceptCL.Instance] = &Assignation{acceptCL.CLId, acceptCL.Ballot, 0, ACCEPTED}
		aclrep.Instance = acceptCL.Instance
		aclrep.OK = TRUE
		aclrep.Ballot = acceptCL.Ballot
		r.acceptedCL[acceptCL.CLId]++
	} else if acceptCL.Ballot < r.assign[acceptCL.Instance].ballot {
		aclrep.Instance = acceptCL.Instance
		aclrep.OK = FALSE
		aclrep.Ballot = r.assign[acceptCL.Instance].ballot
	} else if r.assign[acceptCL.Instance].status == COMMITTED {
		return
	} else {
		r.assign[acceptCL.Instance].commandLeader = acceptCL.CLId
		r.assign[acceptCL.Instance].ballot = acceptCL.Ballot
		r.assign[acceptCL.Instance].status = ACCEPTED
		aclrep.Instance = acceptCL.Instance
		aclrep.OK = TRUE
		aclrep.Ballot = acceptCL.Ballot
	}
	//aclreply = &newmodelproto.AcceptCLReply{acceptCL.Instance, TRUE, acceptCL.Ballot}

	if aclrep.OK == TRUE {
		if aclrep.Instance > r.acceptedUpToCL {
			r.acceptedUpToCL = aclrep.Instance
		}
		if acceptCL.CLId == r.Id {
			r.handleAcceptCLReply(&aclrep)
			return
		}
		r.replyAcceptCL(acceptCL.CLId, &aclrep)
	} else {
		r.replyAcceptCL(acceptCL.LeaderId, &aclrep)
	}
}

func (r *Replica) handleCommitCL(commitCL *newmodelproto.CommitCL) {
	if r.assign[commitCL.Instance] == nil {
		r.assign[commitCL.Instance] = &Assignation{commitCL.CLId, commitCL.Ballot, 0, COMMITTED}
		r.acceptedCL[commitCL.CLId]++
	} else {
		r.assign[commitCL.Instance].commandLeader = commitCL.CLId
		r.assign[commitCL.Instance].status = COMMITTED
	}
	if commitCL.Instance > r.acceptedUpToCL {
		r.acceptedUpToCL = commitCL.Instance
	}
}

func (r *Replica) handlePrepareCLReply(prepareCLReply *newmodelproto.PrepareCLReply) {
	if !r.isLeader {
		return
	}
	//TODO
	if r.assign[prepareCLReply.Instance].status != PREPARING {
		return
	}
	if prepareCLReply.CLId != -1 {
		r.assign[prepareCLReply.Instance].commandLeader = prepareCLReply.CLId
		r.acceptedCL[prepareCLReply.CLId]++
		if prepareCLReply.CLId == r.Id {
			r.increment <- true
		}
	}
	r.assign[prepareCLReply.Instance].status = ACCEPTED
	r.bcastAcceptCL(prepareCLReply.Instance, r.assign[prepareCLReply.Instance].ballot, r.assign[prepareCLReply.Instance].commandLeader)
	r.gapChan <- true
}

func (r *Replica) handleAcceptCLReply(aclreply *newmodelproto.AcceptCLReply) {
	if aclreply.OK == FALSE {
		if !r.isLeader {
			return
		} else {
			return
		}
	} else if r.assign[aclreply.Instance] == nil {
		r.assign[aclreply.Instance] = &Assignation{r.Id, aclreply.Ballot, 1, ACCEPTED}
		r.acceptedCL[r.Id]++
	} else if r.assign[aclreply.Instance].status == COMMITTED {
		return
	} else {
		r.assign[aclreply.Instance].assignOKs++
	}
	if aclreply.Instance > r.acceptedUpToCL {
		r.acceptedUpToCL = aclreply.Instance
	}
	if r.assign[aclreply.Instance].assignOKs+1 > r.N>>1 {
		r.assign[aclreply.Instance].status = COMMITTED
		r.bcastCommitCL(aclreply.Instance, r.Id, aclreply.Ballot)
	}
}

func (r *Replica) runCL() {
	for !r.Shutdown {

		select {
		case prepareCLS := <-r.prepareCLChan:
			prepareCL := prepareCLS.(*newmodelproto.PrepareCL)
			dlog.Printf("Received PrepareCL from replica %d, for instance %d\n", prepareCL.LeaderId, prepareCL.Instance)
			r.handlePrepareCL(prepareCL)
			break

		case acceptCLS := <-r.acceptCLChan:
			acceptCL := acceptCLS.(*newmodelproto.AcceptCL)
			dlog.Printf("Received AcceptCL from replica %d, for instance %d\n", acceptCL.LeaderId, acceptCL.Instance)
			r.handleAcceptCL(acceptCL)
			break

		case prepareCLReplyS := <-r.prepareCLReplyChan:
			prepareCLReply := prepareCLReplyS.(*newmodelproto.PrepareCLReply)
			dlog.Printf("Received PrepareCLReply for instance %d\n", prepareCLReply.Instance)
			r.handlePrepareCLReply(prepareCLReply)
			break

		case acceptCLReplyS := <-r.acceptCLReplyChan:
			acceptCLReply := acceptCLReplyS.(*newmodelproto.AcceptCLReply)
			dlog.Printf("Received AcceptCLReply for instance %d\n", acceptCLReply.Instance)
			r.handleAcceptCLReply(acceptCLReply)
			break

		case commitCLS := <-r.commitCLChan:
			commitCL := commitCLS.(*newmodelproto.CommitCL)
			dlog.Printf("Received CommitCL from replica %d, for instance %d\n", commitCL.CLId, commitCL.Instance)
			r.handleCommitCL(commitCL)
			break
		}
	}
}
