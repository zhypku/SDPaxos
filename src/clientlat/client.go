package main

import (
	"log"
	//	"dlog"
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"masterproto"
	"math/rand"
	"net"
	"net/rpc"
	"runtime"
	"state"
	"time"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var procs *int = flag.Int("p", runtime.NumCPU(), "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var barOne = flag.Bool("barOne", false, "Sent commands to all replicas except the last one.")
var forceLeader = flag.Int("l", -1, "Force client to talk to a certain replica.")
var startRange = flag.Int("sr", 0, "Key range start")
var sleep = flag.Int("sleep", 0, "Sleep")
var T = flag.Int("T", 1, "Number of threads (simulated clients).")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")

var rarray []int
var rsp []bool

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	leader := 0
	if *noLeader == false && *forceLeader < 0 {
		reply := new(masterproto.GetLeaderReply)
		if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
			log.Fatalf("Error making the GetLeader RPC\n")
		}
		leader = reply.LeaderId
		//log.Printf("The leader is replica %d\n", leader)
	} else if *forceLeader > 0 {
		leader = *forceLeader
	}

	done := make(chan bool, *T)

	readings := make(chan float64, 2**reqsNb)

	clock := make(chan bool, 1)

	go printer(readings, done, clock)
	//go printer(readings, done)

	go func(c chan bool) {
		for {
			//time.Sleep(1000 * 1000 * 1000)
			time.Sleep(10 * 1000 * 1000)
			c <- true
		}
	}(clock)

	for i := 0; i < *T; i++ {
		go simulatedClient(rlReply, leader, readings, done, i * *reqsNb)
	}

	for i := 0; i < *T+1; i++ {
		<-done
	}
	master.Close()
}

func simulatedClient(rlReply *masterproto.GetReplicaListReply, leader int, readings chan float64, done chan bool, start int) {
	N := len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	rarray := make([]int, *reqsNb)
	karray := make([]int64, *reqsNb)
	put := make([]bool, *reqsNb)
	perReplicaCount := make([]int, N)
	M := N
	if *barOne {
		M = N - 1
	}
	randObj := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNb))
	for i := 0; i < len(rarray); i++ {
		r := rand.Intn(M)

		rarray[i] = r
		perReplicaCount[r]++

		if *conflicts >= 0 {
			r = rand.Intn(100)
			if r < *conflicts {
				karray[i] = 42
			} else {
				karray[i] = int64(start + *startRange + 43 + i)
			}
			r = rand.Intn(100)
			if r < *writes {
				put[i] = true
			} else {
				put[i] = false
			}
		} else {
			karray[i] = int64(zipf.Uint64())
		}
	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	var id int32 = 0
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, state.NIL}, 0}
	var reply genericsmrproto.ProposeReplyTS

	n := *reqsNb

	for i := 0; i < n; i++ {
		if *noLeader {
			leader = rarray[i]
		}
		//args.ClientId = id
	//		var values [128]int64
	//		for index := range values {
	//			values[index] = int64(i)
	//		}
		args.Command.K = state.Key(karray[i])
		args.Command.V = state.Value(i)
		if !put[i] {
			args.Command.Op = state.GET
		}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)

		before := time.Now()

		args.Marshal(writers[leader])
		writers[leader].Flush()
		if err := reply.Unmarshal(readers[leader]); err != nil || reply.OK == 0 {
			fmt.Println("Error when reading:", err)
			continue
		}

		after := time.Now()

		id++

		readings <- (after.Sub(before)).Seconds() * 1000

		if *sleep > 0 {
			for i := 0; i < *sleep; i++ {
				time.Sleep(100 * 1000 * 1000)
			}
		}
	}

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	done <- true
}

func printer(readings chan float64, done chan bool, clock chan bool) {
	n := *T * *reqsNb
	successful := 0
	successfulPrev := 0
	latency := float64(0)
	//latencyPrev := float64(0)
	before := time.Now()
	for successful < n {
		select {
		case lat := <-readings:
			//fmt.Printf("%v\n", lat)
			latency += lat
			successful++

		case <-clock:
			//fmt.Printf("Throughput %v\n", successful-successfulPrev)
			fmt.Printf("%v\n", (successful-successfulPrev)*10)
			//fmt.Printf("Latency %v\n", (latency-latencyPrev)/float64(successful-successfulPrev))
			successfulPrev = successful
			//latencyPrev = latency

		}
	}
	after := time.Now().Sub(before).Seconds()
	tp := float64(successful) / after
	fmt.Printf("Average throughput %v\n", tp)
	fmt.Printf("Average latency %v\n", latency/float64(successful))
	done <- true
}
/*func printer(readings chan float64, done chan bool) {
	n := *T * *reqsNb
	for i := 0; i < n; i++ {
		lat := <-readings
		fmt.Printf("%v\n", lat)
	}
	done <- true
}*/
