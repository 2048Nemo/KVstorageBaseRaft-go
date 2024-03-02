package kvserver

import (
	"KVstorageBaseRaft-go/kvnode"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
)

type KVserver struct {
	mu         sync.Mutex
	node       *kvnode.Raft
	me         int64
	storage    map[string]int
	applychan  chan kvnode.ApplyMsg
	chanmap    map[int]chan OpMsg
	lis        net.Listener
	grpcServer *grpc.Server
	filePath   string
	killed     bool
}

type OpMsg struct {
	Index int64
	Succ  bool
	Op    string
	Key   string
	Value int64
	// fail massage
	Msg string
}

func NewKVServer(me int, peers []int, addrs []string, persist string) *KVserver {
	applych := make(chan kvnode.ApplyMsg)
	fmt.Printf("[Server %v] is making node\n", me)
	node := kvnode.NewRaft(me, peers, addrs, applych)
	chmap := make(map[int]chan OpMsg)
	storage := make(map[string]int)

	lis, err := net.Listen("tcp", addrs[me])
	if err != nil {
		panic("listen failed\n")
	}
	grpcserver := grpc.NewServer()
	kvnode.RegisterRaftKVServer(grpcserver, node)
	go func() {
		err := grpcserver.Serve(lis)
		if err != nil {
			fmt.Print(errors.Wrap(err, "启动rpc服务器失败\n"))
		}
	}()
	kvserver := &KVserver{
		mu:         sync.Mutex{},
		node:       node,
		me:         int64(me),
		storage:    storage,
		applychan:  applych,
		chanmap:    chmap,
		lis:        lis,
		grpcServer: grpcserver,
		filePath:   persist,
		killed:     false,
	}
	return kvserver
}
func (kv *KVserver) Connect() {
	fmt.Printf("正在连接到服务器节点集群:\n")
	kv.node.Connect()
	go kv.ticker()
}

func (kv *KVserver) ticker() {
	for kv.killed != true {
		//监听raft中的 apply管道的提交信息
		applyMsg := <-kv.applychan
		//将数据提交到storage持久化到kv服务器上
		opmsg := kv.apply(&applyMsg)
		index := opmsg.Index
		//获取回复通道
		ch := kv.getReplyChan(index)
		if ch != nil {
			fmt.Printf("[Server %v] is sending msg to DBSevice\n", kv.me)
			ch <- opmsg
			fmt.Printf("[Server %v] finished sending msg to DBSevice\n", kv.me)
		}
	}
}

// 将存储节点已提交的数据持久化
func (kv *KVserver) apply(msg *kvnode.ApplyMsg) OpMsg {
	fmt.Printf("[Server %v] is applying %v\n", kv.me, msg.Index)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := &OpMsg{
		Index: int64(msg.Index),
		Succ:  true,
		Op:    msg.Op,
		Key:   msg.Key,
		Value: int64(msg.Value),
	}

	switch op.Op {
	case "update", "put":
		kv.storage[msg.Key] = int(msg.Value)
		_, succ := kv.storage[msg.Key]
		if succ {
			fmt.Printf("[Server %v] %v the %v:%v successfully\n", kv.me, msg.Op, msg.Key, msg.Value)
		}
	case "remove":
		_, succ := kv.storage[msg.Key]
		if succ {
			delete(kv.storage, msg.Key)
			fmt.Printf("[Server %v] %v the %v:%v delete successfully\n", kv.me, msg.Op, msg.Key, msg.Value)
		} else {
			op.Succ = false
			op.Msg = fmt.Sprintf("delete fail: there is no %v\n", msg.Key)
		}
	case "get":
		value, succ := kv.storage[msg.Key]
		if succ {
			op.Value = int64(value)
			fmt.Printf("[Server %v] get %v successfully\n", kv.me, msg.Key)
		} else {
			op.Succ = false
			op.Msg = fmt.Sprintf("get fail: there is no %v\n", msg.Key)
		}
	}
	fmt.Printf("[Server %v] finish apply\n", kv.me)
	return *op
}

func (kv *KVserver) getReplyChan(index int64) chan OpMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chanmap[int(index)]; !ok {
		return nil
	}
	return kv.chanmap[int(index)]
}

func (kv *KVserver) Exec(ch chan OpMsg, op string, key string, value int64) {
	fmt.Printf("[Server %v] receive a op request : %v %v\n", kv.me, op, key)

	index, term, isleader := kv.node.Exec(op, key, value)
	println("[Server %v] executive this  request")
	if index != -1 && term != -1 && isleader {
		kv.mu.Lock()
		kv.chanmap[index] = ch
		kv.mu.Unlock()
	} else if isleader == false {
		fmt.Printf("该节点现在不是leader了\n")
	}
}

func (kv *KVserver) close() error {

	//先将raft节点关闭再关闭 kv服务器
	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()
	err := kv.node.Close()
	if err != nil {
		panic(err)
	}

	kv.grpcServer.Stop()

	err = kv.lis.Close()
	if err != nil {
		panic(err)
	}
	err = kv.persist()
	if err != nil {
		panic(err)
	}
	return nil
}

func (kv *KVserver) persist() error {
	bytes, err := json.Marshal(kv.storage)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[Server %v] 's storage is %+v\n", kv.me, kv.storage)
	file, err := os.OpenFile(kv.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	_, err = file.Write(bytes)
	if err != nil {
		panic(err)
	}
	return nil
}

func (server *KVserver) IsLeader() bool {
	return server.node.IsLeader()
}
