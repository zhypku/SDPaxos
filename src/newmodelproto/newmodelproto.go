package newmodelproto

import (
	"state"
)

type Prepare struct {
	LeaderId   int32
	Instance   int32
	Ballot     int32
	ToInfinity uint8
}

type PrepareReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
	Command  []state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type AcceptReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
}

type PrepareCL struct {
	LeaderId int32
	Instance int32
	Ballot   int32
}

type PrepareCLReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
	CLId     int32
}

type AcceptCL struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	CLId     int32
}

type CommitCL struct {
	CLId     int32
	Instance int32
	Ballot   int32
}

type AcceptCLReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
}

type RequestCL struct {
	LeaderId int32
	IsRead   uint8
	Object   int64
	Instance int32
}

type RequestReply struct {
	ReadSeq  int32
	Instance int32
	LeaderId int32
}

type AskForVote struct {
	Candidate int32
	Term      int32
}

type Vote struct {
	VoterId        int32
	OK             uint8
	AcceptedUpTo   []int32
	AcceptedUpToCL int32
}

type Winning struct {
	LeaderId int32
	Term     int32
}
