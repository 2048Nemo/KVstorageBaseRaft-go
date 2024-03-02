package kvserver

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"sync"
	"time"
)

type DBService interface {
	Get(key string) (int, error)
	Put(key string, value int64) error
	Update(key string, value int64) error
	Remove(key string) error
	Close() error
}

type KVService struct {
	mu        sync.Mutex
	clientnum int
	servers   []*KVserver
	closed    bool
}

type Config struct {
	Mode          string   `yaml:"mode"`
	ServersNum    int      `yaml:"serversNum"`
	Address       []string `yaml:"address"`
	Port          []int    `yaml:"port"`
	PersistPath   []string `yaml:"persistPath"`
	ListenAddress string   `yaml:"listenAddress"`
	ListenPort    string   `yaml:"listenPort"`
}

func NewDBServerWithConfig() *KVService {
	var config Config
	configdata, err := os.ReadFile("./config.yaml")
	if err != nil {
		config.Mode = "default"
	}

	err = yaml.Unmarshal(configdata, &config)
	if err != nil {
		panic(err)
	}
	num := 0
	if config.Mode == "default" {
		// use 3 servers to test
		num = 3
	} else {
		num = config.ServersNum
	}

	addrs := make([]string, num)
	peers := make([]int, num)
	servers := make([]*KVserver, num)
	persists := make([]string, num)
	if config.Mode == "default" {
		addrs[0] = "127.0.0.1:10001"
		addrs[1] = "127.0.0.1:10002"
		addrs[2] = "127.0.0.1:10003"
		peers[0] = 0
		peers[1] = 1
		peers[2] = 2
		persists[0] = "./temp/temp1.json"
		persists[1] = "./temp/temp2.json"
		persists[2] = "./temp/temp3.json"
	} else {
		for i := 0; i < num; i++ {
			peers[i] = i
			addrs[i] = fmt.Sprintf("%v:%v", config.Address[i], config.Port[i])
			persists[i] = config.PersistPath[i]
		}
	}

	for i := 0; i < num; i++ {
		server := NewKVServer(i, peers, addrs, persists[i])
		servers[i] = server
	}
	DB := &KVService{
		clientnum: 0,
		servers:   servers,
		closed:    false,
	}
	for i := 0; i < num; i++ {
		servers[i].Connect()
	}
	return DB
}

func (s *KVService) Get(key string) (int, error) {
	channel := make(chan OpMsg)
	find := false
	for {
		if find {
			break
		}
		for i := range s.servers {
			if s.servers[i].IsLeader() {
				s.mu.Lock()
				s.clientnum++
				fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
				s.mu.Unlock()
				s.servers[i].Exec(channel, "get", key, 0)
				find = true
				break
			}
		}
	}
	msg := <-channel
	s.mu.Lock()
	s.clientnum--
	fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
	s.mu.Unlock()
	if !msg.Succ {
		fmt.Println(msg.Msg)
	} else {
		fmt.Printf("DBService get the value of %v:%v\n", key, msg.Value)
	}
	return int(msg.Value), nil
}

func (s *KVService) Put(key string, value int64) error {
	fmt.Printf("[DBService] receive a put request\n")
	channel := make(chan OpMsg)
	find := false
	for {
		if find {
			break
		}
		for i := range s.servers {
			if s.servers[i].IsLeader() {
				fmt.Printf("[DBService] find leader %v\n", i)
				s.mu.Lock()
				s.clientnum++
				fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
				s.mu.Unlock()
				s.servers[i].Exec(channel, "put", key, value)
				fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
				find = true
				break
			}
		}
	}

	msg := <-channel
	s.mu.Lock()
	s.clientnum--
	fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
	s.mu.Unlock()
	if msg.Succ {
		fmt.Println("DBService finish put operation")
	} else {
		fmt.Println(msg.Msg)
	}
	return nil
}

func (s *KVService) Update(key string, value int64) error {
	fmt.Printf("[DBService] receive a update request\n")
	channel := make(chan OpMsg)
	find := false
	for {
		if find {
			break
		}
		for i := range s.servers {
			if s.servers[i].IsLeader() {
				fmt.Printf("[DBService] find leader %v\n", i)
				s.mu.Lock()
				s.clientnum++
				fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
				s.mu.Unlock()
				s.servers[i].Exec(channel, "update", key, value)
				find = true
			}
		}
	}
	msg := <-channel
	s.mu.Lock()
	s.clientnum--
	fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
	s.mu.Unlock()
	if msg.Succ {
		fmt.Println("DBService finish put operation")
	} else {
		fmt.Println(msg.Msg)
	}
	return nil
}

func (s *KVService) Remove(key string) error {
	fmt.Printf("[DBService] receive a update request\n")
	channel := make(chan OpMsg)
	find := false
	for {
		if find {
			break
		}
		for i := range s.servers {
			if s.servers[i].IsLeader() {
				fmt.Printf("[DBService] find leader %v\n", i)
				s.mu.Lock()
				s.clientnum++
				fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
				s.mu.Unlock()
				s.servers[i].Exec(channel, "delete", key, 0)
				find = true
			}
		}
	}
	msg := <-channel
	s.mu.Lock()
	s.clientnum--
	fmt.Printf("[DBService] clientnum is %v\n", s.clientnum)
	s.mu.Unlock()
	if msg.Succ {
		fmt.Println("DBService finish put operation")
	} else {
		fmt.Println(msg.Msg)
	}
	return nil
}

func (s *KVService) Close() error {
	//避免关闭过快
	time.Sleep(5 * time.Second)
	//等待5秒等所有服务器节点数据落盘，之后会一直占有锁，检测操作是否都做完了
	fmt.Println("DBService is closing")
	for {
		s.mu.Lock()
		if s.clientnum == 0 {
			fmt.Printf("[DBService] can Close now\n")
			s.closed = true
			break
		}
		s.mu.Unlock()
	}
	for i := range s.servers {
		err := s.servers[i].close()
		if err != nil {
			return err
		}
	}
	fmt.Println("DB service has been closed()")
	return nil
}
