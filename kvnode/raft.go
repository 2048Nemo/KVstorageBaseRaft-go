package kvnode

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
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
	mu sync.Mutex // 锁

	// peers record the index of client
	// conns is used to manage  connections
	// stubs is used to Calling service methods
	peers []int
	conns []*grpc.ClientConn
	stubs []*RaftKVClient
	addrs []string

	persister *Persister // 持久化信息
	me        int        // 本节点在成员通讯信息的index

	//Persistent state on all servers
	//所有服务器都有的持久性状态（在响应RPC请求之前必须先保存状态）
	CurrentTerm int         // 目前的Term(初始值为0，单调增加)
	VotedFor    int         // 当前term 内获得投票的候选人的ID（没有则为-1）
	Log         []*LogEntry //log信息:对于每个状态机而言：每个在任期内从leader发送过来的日志包含命令（日志的第一个索引号是 1 ）

	// Volatile state on all servers
	// 所有服务器都有的易失性状态
	CommitIndex int // 目前已经提交的最高的日志索引号 （初始为 0 ，单调递增）
	lastApplied int // 应用于状态机的最高的日志索引号 （初始为 0 ，单调递增）(快照相关，暂时不管)

	// Volatile state on  leaders
	//  leader 特有的易失性状态（选举后会重新初始化）
	nextIndex     map[int]int //对于每台服务器想要获取的下一条日志的索引值，（初始化的这个值为leader 最后一个日志索引 + 1）
	matchIndex    map[int]int //匹配索引,对于每个服务器该属性记录了已经复制到本节点的最高的日志的索引值（初始化为0 ， 单调增加）
	ChanCharacter chan int    //节点改变时的通知chan
	Character     Character   //当前本节点在Raft中的角色

	election_timeout  *time.Timer
	heartbeat_timeout *time.Timer //定时器

	ApplyMsgChan chan ApplyMsg //传输应用的信息的chan,一个对外提供已达成协议的管道。如果某项日志在成员之中通过了，那么就通过此通道告诉使用方。

	PrevSnapIndex int //有快照时，快照保存的最近的idx
	PrevSnapTerm  int //有快照时，快照保存的最近的idx的Term

	killed bool
}

// 与外界传输信息的结构
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool // True安装快照 False为普通请求
	Snapshot    []byte
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
		rf.heartbeat_timeout.Reset(time.Millisecond * 100)
	} else {
		//随机选举超时时间 定义随机间隔为 120-320ms
		timeout := (rand.Intn(300)%300 + 120)
		//在这里发现时间包中的变量直接乘以整数会爆红，应该转化成int64的值
		rf.election_timeout.Reset(time.Millisecond * time.Duration(timeout))
	}
	fmt.Println("当前时间为:", time.Now())
}

func (rf *Raft) ticker() {
	for !rf.killed {
		rf.mu.Lock()
		currentState := rf.Character
		rf.mu.Unlock()
		select {
		case <-rf.heartbeat_timeout.C:
			rf.mu.Lock()
			if currentState == Leader {
				//fmt.Printf("[Node %v] send heart beart", rf.me)
				// 发送心跳检测
				rf.doHeartBeat()
			}
			rf.mu.Unlock()
		case <-rf.election_timeout.C:
			rf.mu.Lock()
			//遵从单一职责原则，不选择在这边重置选举超时
			rf.doElection()
			rf.mu.Unlock()
		}
	}
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
	if rf.CurrentTerm > int(args.Term) {
		reply.Success = false
	}
	//现任当前term leader 调用要求追加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//更新leader的term
	rf.CurrentTerm = int(args.Term)
	//一定要先回退到character follower 角色，因为我写的定时器刷星机制是和角色挂钩的
	rf.Character = Follower
	//刷新定时器
	rf.ResetTimer()

	//如果日志中preLogIndex的Term与PreLogTerm不一样，Success为false(if Log[preLogIndex].Term == PreLogTerm)
	//必须要prelogindex必须要大于传入的prelogindex 否则的话让leader重传更小的
	if len(rf.Log) >= int(args.PrevLogIndex) && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = int64(rf.CurrentTerm)
		reply.Success = false
		return
	}
	//如果日志中preLogIndex的Term与PreLogTerm一样，Success为true
	reply.Success = true
	reply.Term = int64(rf.CurrentTerm)

	//如果是心跳检测的话那将没有log entry
	if len(args.Entries) != 0 {
		//检测冲突(相同的index 但是却是不同的term) ,并解决冲突
		i := args.PrevLogIndex
		for ; i < len(args.LogEntries)+args.PreLogIndex && i <= len(rf.Log); i++ {
			if rf.Log[i].Index == args.LogEntries[i-args.PreLogIndex].Index && rf.Log[i].Term != args.LogEntries[i-args.PreLogIndex].Term {
				//冲突存在的话就以leader发送过来的为准
				*rf.Log[i] = *args.LogEntries[i-args.PreLogIndex]
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

//选举
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
		//当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡住
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

		//生成requestVoteArgs
		lastLogIndex, lastLogTerm := int(-1), int(-1)
		rf.getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm) //获取最后一个log的term和下标

		requestVoteArgs := &RequestVoteArgs{
			Term:         int64(rf.CurrentTerm),
			CandidateId:  int64(rf.me),
			LastLogIndex: int64(lastLogIndex),
			LastLogTerm:  int64(lastLogTerm),
		}

		//lastResetElectionTime := time.Now()
		//	发布RequestVote RPC
		votelock := sync.Mutex{}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				start := time.Now()
				fmt.Printf("[func-sendRequestVote {%d}] 向server{%d} 发送 RequestVote 开始", rf.me, rf.CurrentTerm, rf.getLastLogIndex())

				reply, succ := rf.SendRequestVote(peer, requestVoteArgs)

				fmt.Printf("[func-sendRequestVote {%d}] 向server{%d} 发送 RequestVote 结束，耗时:{%v} ms", rf.me, rf.CurrentTerm, rf.getLastLogIndex(), time.Now().Sub(start))

				if !succ {
					fmt.Printf("RequestVote to server %d failed at %s\n", peer, start.String())
					return
				} else {
					//调用rpc成功说明这次调用网络畅通
					//由于是service端并发的rpc调用所以要加个锁进行同步
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//对回应进行处理，要记得无论什么时候收到回复就要检查term
					if reply.Term > int64(rf.CurrentTerm) {
						//尴尬了别人term大说明集群已经完成选leader了，身份应该从candidate转变为follower
						//三变： 身份，term，和投票,防止遗忘
						rf.Character = Follower
						rf.CurrentTerm = int(reply.GetTerm())
						rf.VotedFor = -1
						rf.ResetTimer()
					} else if reply.GetVoteGranted() {
						//获得选票
						//否则就是接收到别人的票了，现在开始检查票数是否超过半数，是就当选
						votelock.Lock()
						votedNum += 1

						//为什么要+1？因为这个peers 整个集群的数量通常是单数，所以加个1向上取整
						if votedNum >= len(rf.peers)/2+1 {
							fmt.Printf("[Node %v] become a new leader", rf.me)
							rf.ToBeLeader()
						}
						votelock.Unlock()
					}
				}
			}(i)
		}
	}

	return
}

// 如果args.Term < currentTerm, 返回false
// - 如果args.Term > currentTerm, currentTerm = args.Term，改变角色为Follower。如果
// - 如果votedFor为空或者有candidateID，并且候选人的日志至少与接收者的日志一样新，投赞成票并刷新计时器。至少一样新是指：
//
//	args.LastLogIndex > my.LastLogIndex || (args.LastLogIndex == my.LastLogIndex && LastLogTerm >= Log[LastLogIndex].Term)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	//先持久化在解锁
	defer rf.persist()
	defer rf.mu.Unlock()

	//分三种情况讨论args term 和currentTerm 的关系
	//第一种： args 的term选主慢了，网络分区已经完成了选主
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		reply.ok = false
		return false
	}
	if args.Term > rf.CurrentTerm {
		//第二种：args发送过来请求投票,如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
		rf.CurrentTerm = args.Term
		rf.Character = Follower
		rf.VotedFor = -1
		//	重置定时器：收到leader的ae，开始选举，透出票
		//这时候更新了term之后，votedFor也要置为-1
	}
	if rf.CurrentTerm == args.Term {
		fmt.Errorf("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", rf.me)
	}
	//	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)
	//	，要检查log的term和index是不是匹配的了
	//1.如果投票者自己的日志比候选人的日志更新，则投票者会拒绝投票。
	//2.（“更新”的定义：）如果日志的最后一个entry具有不同的term ，则具有较晚term的日志是更加新的。如果日志以相同的term结尾，则较长的日志是更加新的。
	//3.如果投票者的日志没有leader的新的话就会接受leader的日志

	if !(args.LastLogIndex > rf.getLastLogIndex() || (args.LastLogIndex == rf.getLastLogIndex() && args.LastLogTerm >= rf.getLastLogTerm())) {
		//先排除不符合投票条件的candidate发送过来请求
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		reply.ok = false
		//这里返回false 和true 是瞎写的，因为这个是本地调用基本上不会出现调用失败的情况，这个函数返回值仅仅是rpc函数前的测试而已
		return true
	}
	//同时收到多个选举投票请求时（但是票已经投出去了）
	if rf.VotedFor != -1 && args.CandidateID != rf.VotedFor {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		reply.ok = false
		return true
	} else {
		//第一次接收到投票请求或者是重复接收到投票请求（网络故障导致candidate没有收到reply的信息,注意由于上面条件的过滤，集群中能到这边的重复请求只能是最新candidate发送过来的重复请求）
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		reply.ok = true
		return true
	}
}

func (rf *Raft) getLastLogIndexAndTerm(longIndex *int, term *int) {
	//防止一开始（或者已删除导致没有日志了）本机上没有日志，导致访问越界
	if len(rf.Log) == 0 {
		*longIndex = rf.PrevSnapIndex
		*term = rf.PrevSnapTerm
	} else {
		//存在日志可以直接访问获取
		*longIndex = int(rf.Log[len(rf.Log)-1].Index)
		*term = int(rf.Log[len(rf.Log)-1].Term)
	}

	return
}
func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := -1
	term := -1
	rf.getLastLogIndexAndTerm(&lastLogIndex, &term)
	return term
}

func (rf *Raft) getLastLogIndex() int {
	lastLogIndex := -1
	term := -1
	rf.getLastLogIndexAndTerm(&lastLogIndex, &term)
	return lastLogIndex
}

func (rf *Raft) SendRequestVote(serverIdx int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, succ := (*rf.stubs[serverIdx]).RequestVote(ctx, args)
	return reply, succ == nil
}

func (rf *Raft) persist() {

}

func (rf *Raft) doHeartBeat() {
	//先加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Character == Leader {
		//向除了自己以外的所有节点发送消息appendEntry消息
		fmt.Printf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", rf.me)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			fmt.Printf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", rf.me, i)
			if rf.nextIndex[i] >= 1 {
				fmt.Errorf("rf.nextIndex[%d] = {%d}", i, rf.nextIndex[i])
				return
			}

			//比快照索引小则发送快照
			//日志压缩加入后要判断是发送快照还是发送AE
			//注意这里是小于等于后面getPreLogIndex条件会依赖 这个条件
			if rf.nextIndex[i] <= rf.PrevSnapIndex {
				go rf.leaderSendSnapShot()
				continue
			}

			//这两个变量是专门计算将要发送的appendEntries需要的参数
			entryPreLogIndex := -1
			entryPreLogTerm := -1
			//这个本质上就是跟据follower的nextindex计算出来项目中的接下来要发送的ae信息
			rf.getPreLogInfo(i, &entryPreLogIndex, &entryPreLogTerm)
			//发送ae，先生成appendEntry实体 和 appendEntryReply

			//生成logEntries 由nextIndex【server】记录的index -1开始复制到末尾
			logEntries := make([]*LogEntry, len(rf.Log)-int(entryPreLogIndex)-1)
			copy(logEntries, rf.Log[1+entryPreLogIndex:])
			appendEntriesArgs := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				PreLogIndex:  entryPreLogIndex,
				PreLogTerm:   entryPreLogTerm,
				LogEntries:   logEntries,
				LeaderCommit: rf.CommitIndex,
			}

			go func(peer int) {
				reply, ok := rf.SendAppendEntries(peer, appendEntriesArgs)
				if ok {
					if reply.Success {
						//从服务器已经完成日志更新，将更新leader节点的状态（也就是nextIndex和matchIndex 这两个的关系是受互相制约的）
						rf.nextIndex[peer] = appendEntriesArgs.PreLogIndex + len(logEntries)
						rf.matchIndex[peer] = rf.matchIndex[peer] + 1
						fmt.Printf("[Node %v] update %v nextIndex and matchIndex to %v %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
						//同步日志找到集群中最大的那个matchIndex是多少
						toCommit := make([]int, len(rf.peers))
						for peer := range rf.peers {
							matchIndex := rf.matchIndex[peer]
							toCommit[matchIndex]++
						}
						//计算toCommit中最大的是谁，然后就commit它
						peerLen := len(rf.peers)
						// find the largest index which can be committed (at least larger than old commitIndex)
						sum := 0
						for i := len(toCommit) - 1; i > int(rf.CommitIndex); i-- {
							//注意：这边是从已经保存的最大的index中从后往前计算的，累加是由于每个节点保存最大的节点肯定是会包含前面节点的索引的
							sum += toCommit[i]
							if sum >= (peerLen/2)+1 {
								//这里可以加上幂等性操作比如 sum == (peerLen/2) + 1 ,或者是在进入条件后将sum赋值为零，不过由于这边sum是临时的变量几乎不需要考虑幂等性
								rf.CommitIndex = int(i)
								rf.apply()
								fmt.Printf("[Node %v] commitIndex is %v", rf.me, rf.CommitIndex)
								break
							}
						}
					} else {
						//success 返回false 有两个原因 1.args 的prelogIndex比他的prelogIndex大 2.args 的prelogTerm 不是同一个Term，说明发生了选举
						//发现对方term大，说明发生了选举，自动退回到follower角色
						if rf.CurrentTerm < reply.Term {
							rf.Character = Follower
							rf.ResetTimer()
						} else {
							//没有匹配的日志
							rf.nextIndex[peer] = -1
							fmt.Printf("[Node %v] update %v nextIndex to %v", rf.me, peer, rf.nextIndex[peer])
						}

					}
				} else {
					fmt.Printf("[Node %v] lost connection with %v", rf.me, peer)
				}

			}(i)

		}
	}
}

func (rf *Raft) sendHeartBeat() {

}

func (rf *Raft) leaderSendSnapShot() {

}

func (rf *Raft) getPreLogInfo(server int, entryPreLogIndex *int, entryPreLogTerm *int) {
	//logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
	if rf.nextIndex[server] == rf.PrevSnapIndex+1 {
		//有现成的快照索引于是直接返回便是
		*entryPreLogIndex = rf.PrevSnapIndex
		*entryPreLogTerm = rf.PrevSnapTerm
		return
	}
	nextidx := rf.nextIndex[server]
	*entryPreLogIndex = nextidx - 1
	*entryPreLogTerm = int(rf.Log[nextidx].Term)
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, succ := (*rf.stubs[server]).AppendEntries(ctx, args)
	if succ != nil {
		fmt.Printf("rpc call AppendEntries failed: %v\n", succ)
	}
	return reply, succ == nil
}

func (rf *Raft) apply() {

}

func (rf *Raft) ToBeLeader() {
	//	第一次变成leader，初始化状态和nextIndex、matchIndex
	rf.Character = Leader
	rf.VotedFor = -1
	fmt.Printf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", rf.me, rf.CurrentTerm, rf.getLastLogIndex())

	lastLogIndex := rf.getLastLogIndex()
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastLogIndex + 1 //请查看这个变量定义,因此要+1
		rf.matchIndex[i] = 0               //每换一个领导都是从0开始
	}

	go rf.doHeartBeat() //马上向其他节点宣告自己就是leader
}
