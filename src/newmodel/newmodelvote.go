package newmodel

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"log"
	"newmodelproto"
)

var mv genericsmr.Message

func (r *Replica) replyVote(replicaId int32, vote *newmodelproto.Vote) {
	mv.PeerId = replicaId
	mv.Code = r.voteRPC
	mv.Msg = vote
	r.MessageChan <- mv
}

var mafv genericsmr.Message

func (r *Replica) bcastAskForVote() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("AskForVote bcast failed:", err)
		}
	}()

	var afv newmodelproto.AskForVote
	afv.Candidate = r.Id
	afv.Term = r.currentTerm
	mafv.Code = r.askForVoteRPC
	mafv.Msg = &afv

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
		//r.SendMsg(q, r.acceptRPC, args)
		mafv.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mafv
	}
}

var mwin genericsmr.Message

func (r *Replica) bcastWinning() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Winning bast failed:", err)
		}
	}()
	win := &newmodelproto.Winning{r.Id, r.currentTerm}
	mwin.Code = r.winningRPC
	mwin.Msg = win

	q := 0
	for sent := 0; sent < r.N-1; q++ {
		if r.PreferredPeerOrder[q] == r.Id {
			break
		}
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		mwin.PeerId = r.PreferredPeerOrder[q]
		r.MessageChan <- mwin
	}
}

func (r *Replica) handleAskForVote(ask *newmodelproto.AskForVote) {
	r.Alive[r.currentLeader] = false
	vote := &newmodelproto.Vote{r.Id, TRUE, r.acceptedUpTo, r.acceptedUpToCL}
	if ask.Term <= r.currentTerm {
		vote.OK = FALSE
	} else {
		r.currentTerm = ask.Term
	}
	mvote := genericsmr.Message{ask.Candidate, r.voteRPC, vote, false}
	r.MessageChan <- mvote
	var winS fastrpc.Serializable
	if vote.OK == FALSE {
		return
	} else {
		winS = <-r.winningChan
	}
	//TODO

	win := winS.(*newmodelproto.Winning)
	r.currentLeader = win.LeaderId
	r.currentTerm = win.Term
	log.Printf("Now Replica %d has been the leader of term %d\n", r.currentLeader, r.currentTerm)
}

func (r *Replica) handleLeaderFailure() {
	r.Alive[r.currentLeader] = false
	r.currentTerm++
	r.bcastAskForVote()
	votes := make([]*newmodelproto.Vote, r.N>>1)
	for i := 0; i < r.N>>1; i++ {
		voteS := <-r.voteChan
		votes[i] = voteS.(*newmodelproto.Vote)
		dlog.Printf("Receive vote from replica %d\n", votes[i].VoterId)
		//votes[i] = vote.AcceptedUpTo
	}
	maxAccepted := r.acceptedUpTo
	for index, _ := range maxAccepted {
		for _, item := range votes {
			if item.AcceptedUpTo[index] > maxAccepted[index] {
				maxAccepted[index] = item.AcceptedUpTo[index]
			}
		}
	}

	maxCL := r.acceptedUpToCL

	for index, _ := range votes {
		if votes[index].AcceptedUpToCL > maxCL {
			maxCL = votes[index].AcceptedUpToCL
		}
	}
	//TODO:scan the assignation table

	gaps := 0
	for i := int32(0); i <= maxCL; i++ {
		if r.assign[i] == nil {
			gaps++
			r.assign[i] = &Assignation{-1, r.makeUniqueBallot(0), 0, PREPARING}
			r.bcastPrepareCL(i, r.makeUniqueBallot(0))
		}
	}

	for i := 0; i < gaps; i++ {
		<-r.gapChan
	}

	r.currentLeader = r.Id
	r.isLeader = true
	r.nextAssigned = maxCL + 1
	go r.handleRequestCL()

	for index, _ := range maxAccepted {
		toBeAssign := maxAccepted[index] - r.acceptedCL[index]
		for i := int32(0); i < toBeAssign; i++ {
			reqLocal := &newmodelproto.RequestCL{int32(index), FALSE, -1, -1}
			r.requestCLChan <- reqLocal
		}
	}
	r.bcastWinning()

	log.Printf("Now Replica %d has been the leader of term %d\n", r.currentLeader, r.currentTerm)
}
