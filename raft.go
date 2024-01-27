package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type Persister struct {
}

// 定义raft算法中角色的枚举类型

type Character int

const (
	Follower Character = iota
	Candidate
	Leader
)

//	定义raft算法中的对象
//
// 要理解为什么raft 结构体需要这个属性以及方法
type Raft struct {
	mu        sync.Mutex    // 锁
	peers     []*rpc.Client // 所用成员的通讯信息
	persister *Persister    // 持久化信息
	me        int           // 本节点在成员通讯信息的index

	//Persistent state on all servers
	//所有服务器都有的持久性状态（在响应RPC请求之前必须先保存状态）
	CurrentTerm int        // 目前的Term(初始值为0，单调增加)
	VotedFor    int        // 当前term 内获得投票的候选人的ID（没有则为0）
	Log         []LogEntry //log信息:对于每个状态机而言：每个在任期内从leader发送过来的日志包含命令（日志的第一个索引号是 1 ）

	// Volatile state on all servers
	// 所有服务器都有的易失性状态
	CommitIndex int // 目前已经提交的最高的日志索引号 （初始为 0 ，单调递增）
	lastApplied int // 应用于状态机的最高的日志索引号 （初始为 0 ，单调递增）(快照相关，暂时不管)

	// Volatile state on  leaders
	//  leader 特有的易失性状态（选举后会重新初始化）
	nextIndex     map[int]int   //对于每台服务器想要获取的下一条日志的索引值，（初始化的这个值为leader 最后一个日志索引 + 1）
	matchIndex    map[int]int   //匹配索引,对于每个服务器该属性记录了已经复制到本节点的最高的日志的索引值（初始化为0 ， 单调增加）
	ChanCharacter chan int      //节点改变时的通知chan
	Character     Character     //当前本节点在Raft中的角色
	TimeRest      *time.Timer   //定时器
	ApplyMsgChan  chan ApplyMsg //传输应用的信息的chan,一个对外提供已达成协议的管道。如果某项日志在成员之中通过了，那么就通过此通道告诉使用方。

	PrevSnapIndex int //有快照时，快照保存的最近的idx
	PrevSnapTerm  int //有快照时，快照保存的最近的idx的Term
}

// 与外界传输信息的结构
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool // True安装快照 False为普通请求
	Snapshot    []byte
}

// log中包含的信息  Command中包含着需要达成一致的东西
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// Raft 对外提供的接口

// 返回当前状态 1.term 2.该服务器认为自己是否是leader
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.Character == Leader
}

// 输入参数为必须要达到一致的日志内容 返回term ，日志的index以及自己认为是否是leader
func (rf *Raft) Start(command interface{}) (term int, index int, isleader bool) {
	//检查log中是否存在command
	return
}

func (rf *Raft) ResetTimer() {
	//一般来说 raft 中的定时器用来心跳检测和随机超时选举，这两个时间相对而言:leader角色用作心跳检测时候会比较只比广播时间（即来回通信的时间）稍微长一点点，毕竟要保证效率
	//而随机超时选举时间会长很多，因为选举频率高的话，那么整个集群可用时间就会降低，那么选票的瓜分就会增大，这样就会导致选举的效率降低
	//一般而言 广播时间(0.5ms-20ms) < 心跳检测时间 < 随机超时选举时间(10ms - 500 ms) < MTBF(Mean Time Before Failure)平均故障时间 一般而言服务器这个指数为几个月甚至更长

	fmt.Println("定时器reset，当前时间为：", time.Now())
	if rf.Character == Leader {
		//心跳间隔时间
		rf.TimeRest = time.NewTimer(time.Millisecond * 100)
	} else if rf.Character == Follower {
		//选举超时时间 定义间隔为300ms
		rf.TimeRest = time.NewTimer(time.Millisecond * 300)
	} else if rf.Character == Candidate {
		//随机选举超时时间 定义随机间隔为 100-300ms
		timeout := (rand.Intn(300)%300 + 100)
		//在这里发现时间包中的变量直接乘以整数会爆红，应该转化成int64的值
		rf.TimeRest = time.NewTimer(time.Millisecond * time.Duration(timeout))
	}
	fmt.Println("当前时间为:", time.Now())
}

// Raft 对内的接口
// todo 后续讲这部分接口使用rpc实现
type AppendEntriesReply struct {
	Term          int  //currentTerm, for leader to update itself
	Success       bool //return true if follower contained entry matching prevLogIndex and prevLogTerm
	isOk          bool // rpc is ok
	ConflictIndex int
	ConflictTerm  int
}

type AppendEntriesArgs struct {
	Term         int        //current Term
	LeaderID     int        // LeaderID
	PreLogIndex  int        //index of log entry immediately preceding new ones
	PreLogTerm   int        //term of prevLogIndex entry
	LogEntries   []LogEntry //log entries to store, request is heart beat if it's empty
	LeaderCommit int        //leader’s CommitIndex
}

// 主要作用是Leader向Follower添加Entry到Follower的日志当中，或者发送心跳包。在返回的reply中，Follower需要返回的当前的Term，出否成功等信息。
// 发送方(Leader)：
// - 如果(nextIndex[FollowerID]已经没有新的日志需要传递了，args.LogEntries 置为空，否则传送从index从nextIndex[FollowerID]到自己最近的Log)
// - LeaderCommit置为目前已经达成一致的日志的Index(commitIndex)。
//
// 接收方(Follower)：
// - 如果当前currentTerm > Term, 返回false。如果currentTerm < Term，将自己的currentTerm置为Term
// - 刷新定时器
// - 如果日志中preLogIndex的Term与PreLogTerm不一样，Success为false(if Log[preLogIndex].Term == PreLogTerm)
// - 如果已存在的日志与新来的这条冲突（相同的index不同的term），删除现有的entry,按照leader发送过来为准
// - 将所有新的日志项都追加到自己的日志中
// - LeaderCommit > commitIndex,将commitIndex = min(leaderCommit, 最新日志项index)
//
// 发送方(Leader)：
// - 如果返回的Term > currentTerm, 将自己的角色置为Follower， currentTerm置为Term。不在处理下面的流程。
// - 如果Success为false，将这个Follower的nextIndex进行减一。如果为true，那么就把这个Follower的nextIndex值加(nextIndex[FollowerID] += len(args.LogEntries))
// - 达成一致后选取达成一致(quorum)的index置为commitIndex

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//判断方法调用方是否是leader（某些情况下服务器故障会导致集群重新选举，而老的leader并不知情，会再次调用这个方法意图恢复心跳检测或者追加日志操作，但是这个返回结果会告诉调用方自己已经不是leader了，从而使调用方回退到follower角色）
	if rf.CurrentTerm > args.Term {
		reply.Success = false
	}
	//现任当前term leader 调用要求追加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//更新leader的term
	rf.CurrentTerm = args.Term
	//一定要先回退到character follower 角色，因为我写的定时器刷星机制是和角色挂钩的
	rf.Character = Follower
	//刷新定时器
	rf.ResetTimer()

	//如果日志中preLogIndex的Term与PreLogTerm不一样，Success为false(if Log[preLogIndex].Term == PreLogTerm)
	//必须要prelogindex必须要大于传入的prelogindex 否则的话让leader重传更小的
	if len(rf.Log) >= args.PreLogIndex && rf.Log[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	//如果日志中preLogIndex的Term与PreLogTerm一样，Success为true
	reply.Success = true
	reply.Term = rf.CurrentTerm

	//如果是心跳检测的话那将没有log entry
	if len(args.LogEntries) != 0 {
		//检测冲突(相同的index 但是却是不同的term) ,并解决冲突
		i := args.PreLogIndex
		for ; i < len(args.LogEntries)+args.PreLogIndex && i <= len(rf.Log); i++ {
			if rf.Log[i].Index == args.LogEntries[i-args.PreLogIndex].Index && rf.Log[i].Term != args.LogEntries[i-args.PreLogIndex].Term {
				//冲突存在的话就以leader发送过来的为准
				rf.Log[i] = args.LogEntries[i-args.PreLogIndex]
				rf.nextIndex[rf.me] = rf.Log[i].Index + 1
				rf.matchIndex[rf.me] = rf.Log[i].Index
			}
		}
		//append 新添加的日志
		for ; i < len(args.LogEntries)+args.PreLogIndex; i++ {
			rf.Log = append(rf.Log, args.LogEntries[i])
			rf.nextIndex[rf.me] = rf.Log[i].Index + 1
			rf.matchIndex[rf.me] = rf.Log[i].Index
		}
	}

	//复制从leader 发来的log 后再更新已提交信息
	// LeaderCommit > commitIndex,将commitIndex = min(leaderCommit, 最新日志项index)

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf.matchIndex[rf.me])
	}

	return
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	ok          bool // rpc is ok
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int //candidate requesting vote
	LastLogIndex int //index of candidate
	LastLogTerm  int //term of candidate's last log entry
}

//发送方(Candidate)：
//- 增加currentTerm
//- 自己先投自己一票
//- 重置计时器
//- 向其他服务器发送RequestVote
//
//接收方:
//- 如果args.Term < currentTerm, 返回false
//- 如果args.Term > currentTerm, currentTerm = args.Term，改变角色为Follower。如果
//- 如果votedFor为空或者有candidateID，并且候选人的日志至少与接收者的日志一样新，投赞成票并刷新计时器。至少一样新是指：
//
//    args.LastLogIndex > my.LastLogIndex || (args.LastLogIndex == my.LastLogIndex && LastLogTerm >= Log[LastLogIndex].Term)
//发送方(Candidate)：
//- 如果收到的投票中term大于curTerm，curTerm = term，转变为Follower
//- 如果投票RPC收到了来自多数服务器的票，当选leader。
//- 如果收到了来自新Leader的AppendEntries RPC（term不小于curTerm），转变为follower
//- 如果选举超时，开始新一轮的选举

// 发送方
func (rf *Raft) doElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Character == Leader {
		fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
	}
	fmt.Printf("[       ticker-func-rf(%v)              ] get the  lock\n", rf.me)

	if rf.Character != Leader {
		fmt.Printf("[       ticker-func-rf(%v)              ]  选举定时器到期且不是leader，开始选举 \n", rf.me)
		//当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
		//重竞选超时，term也会增加的
		//开始换届
		rf.CurrentTerm = rf.CurrentTerm + 1
		//先给自己投一票
		rf.VotedFor = rf.me //即是自己给自己投，也避免candidate给同辈的candidate投
		//重置计时器
		//调用这个方法的必定是从follower 想变成 candidate 的所以应该在投票的同时将 character 转换成 candidate
		rf.Character = Candidate
		//todo 持久化persist（）实现
		rf.ResetTimer()

		votedNum := 1 //投票数

		//lastResetElectionTime := time.Now()
		//	发布RequestVote RPC
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastLogIndex, lastLogTerm := int(-1), int(-1)
			rf.getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm) //获取最后一个log的term和下标

			requestVoteArgs := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			requestVoteReply := &RequestVoteReply{}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go rf.sendRequestVote(rf.me, requestVoteArgs, requestVoteReply, &votedNum, &wg)
			wg.Wait()
		}
	}

	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

}

func (rf *Raft) getLastLogIndexAndTerm(longIndex *int, term *int) {
	if len(rf.Log) == 0 {
		*longIndex = rf.PrevSnapIndex
		*term = rf.PrevSnapTerm
	} else {
		*longIndex = rf.Log[len(rf.Log)-1].Index
		*term = rf.Log[len(rf.Log)-1].Term
	}

	return
}

func (rf *Raft) getLastLogIndex() int {
	lastLogIndex := -1
	term := -1
	rf.getLastLogIndexAndTerm(&lastLogIndex, &term)
	return lastLogIndex
}

func (rf *Raft) sendRequestVote(serverIdx int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int, wg *sync.WaitGroup) bool {
	defer wg.Done()
	//本函数返回的bool判断是网络是否正常通信的，而不是requestVote rpc是否投票的rpc
	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	start := time.Now()
	fmt.Printf("[func-sendRequestVote {%d}] 向server{%d} 发送 RequestVote 开始", rf.me, rf.CurrentTerm, rf.getLastLogIndex())
	ok := rf.peers[serverIdx].Call("RequestVote", args, reply)
	fmt.Printf("[func-sendRequestVote {%d}] 向server{%d} 发送 RequestVote 结束，耗时:{%v} ms", rf.me, rf.CurrentTerm, rf.getLastLogIndex(), time.Now().Sub(start))

	if ok != nil {
		fmt.Printf("RequestVote to server %d failed at %s\n", serverIdx, start.String())
		return false
	}

	//调用rpc成功说明这次调用网络畅通
	//由于是service端并发的rpc调用所以要加个锁进行同步
	rf.mu.Lock()
	//对回应进行处理，要记得无论什么时候收到回复就要检查term
	if reply.Term < rf.CurrentTerm {
		//尴尬了别人term大说明集群已经完成选leader了，身份应该从candidate转变为follower
		//三变： 身份，term，和投票,防止遗忘
		rf.Character = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return true
	} else if rf.CurrentTerm < reply.Term {
		return true
	}

	if reply.VoteGranted == false {
		return true
	}

	//否则就是接收到别人的票了，现在开始检查票数是否超过半数，是就当选
	*voteNum += 1
	//为什么要+1？因为这个peers 整个集群的数量通常是单数，所以加个1向上取整
	if *voteNum >= len(rf.peers)/2+1 {
		//得到大部分支持转变为leader
		*voteNum = 0
		//如果已经是leader 的话就不用设置了，主要是防止并发问题继续重复设置，其实voteNum设置为0 就行了，剩下的一半请求再过来应该也不太可能再leader 初始化一遍了，不过保险起见，别人也是这么写的也保守一点
		if rf.Character == Leader {
			//如果已经是leader了，那么是就是了，不会进行下一步处理了k
			_ = fmt.Errorf("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", rf.me, rf.CurrentTerm)
			return true
		}
		//	第一次变成leader，初始化状态和nextIndex、matchIndex
		rf.Character = Leader
		rf.VotedFor = -1
		fmt.Printf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", rf.me, rf.CurrentTerm, rf.getLastLogIndex())

		lastLogIndex := rf.getLastLogIndex()
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = lastLogIndex + 1 //有效下标从1开始，因此要+1
			rf.matchIndex[i] = 0               //每换一个领导都是从0开始，见fig2
		}
		//todo 待实现
		go rf.doHeartBeat() //马上向其他节点宣告自己就是leader
		//todo 待实现
		rf.persist()
	}
	return true
}

func (rf *Raft) persist() {

}

func (rf *Raft) doHeartBeat() {

}
