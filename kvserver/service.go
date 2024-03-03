package kvserver

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/fs"
	"log"
	"os"
	"reflect"
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

var globalConfigData = []byte(`mode: default
serversNum: 3
address:
- "127.0.0.1"
- "127.0.0.1"
- "127.0.0.1"
port:
- 10001
- 10002
- 10003
peers:
- "0"
- "1"
- "2"
persistPath:
- "./temp/temp1.json"
- "./temp/temp2.json"
- "./temp/temp3.json"
`)

func NewDBServerWithConfig() *KVService {
	var config Config
	var configfile *os.File
	//用作写入初始化的配置文件
	var configdata []byte
	configdata, err := os.ReadFile("./temp/config.yaml")
	if err != nil {
		//没有配置文件的情况，直接创建一个新的文件并写入默认的配置文件
		if reflect.TypeOf(err) == reflect.TypeOf(&fs.PathError{}) {
			//打开文件失败就会创建一个文件
			configfile, err = os.Create("./temp/config.yaml")
			if err != nil {
				log.Fatal(err)
			}
			defer configfile.Close()
			configdata = globalConfigData
			_, err = configfile.Write(configdata)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	err = yaml.Unmarshal(configdata, &config)
	if err != nil {
		fmt.Printf("未解析成功%v", err)
		panic(err)
	}
	num := 0
	num = config.ServersNum
	addrs := make([]string, num)
	peers := make([]int, num)
	servers := make([]*KVserver, num)
	persists := make([]string, num)

	for i := 0; i < num; i++ {
		peers[i] = i
		addrs[i] = fmt.Sprintf("%v:%v", config.Address[i], config.Port[i])
		persists[i] = config.PersistPath[i]
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
		fmt.Println("[DBService] finish put operation")
	} else {
		fmt.Printf("[DBService] not finished put operation:%v", msg.Msg)
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
		fmt.Println("DBService finish update operation")
	} else {
		fmt.Println(msg.Msg)
	}
	return nil
}

func (s *KVService) Remove(key string) error {
	fmt.Printf("[DBService] receive a remove request\n")
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
		fmt.Println("DBService finish remove operation")
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
