package kvnode

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	addrs := make([]string, 3)
	peers := make([]int, 3)

	addrs[0] = "127.0.0.1:10001"
	addrs[1] = "127.0.0.1:10002"
	addrs[2] = "127.0.0.1:10003"
	peers[0] = 0
	peers[1] = 1
	peers[2] = 2

	grpcnode := make([]*grpc.Server, 3)
	servers := make([]*Raft, 3)
	fmt.Printf("Testing Make:\n")

	for index := 0; index < 3; index++ {
		server := NewRaft(index, peers, addrs, make(chan ApplyMsg))

		lis, err := net.Listen("tcp", addrs[index])
		if err != nil {
			panic("listen failed\n")
		}
		grpcServer := grpc.NewServer()
		RegisterRaftKVServer(grpcServer, server)
		go func() {
			err := grpcServer.Serve(lis)
			if err != nil {
				panic(err)
			}
		}()

		grpcnode[index] = grpcServer
		servers[index] = server

	}
	for index := 0; index < 3; index++ {
		servers[index].Connect()
	}
	time.Sleep(1 * time.Second)
	t.Run("init finished", TestTicker)
}

func TestMain(m *testing.M) {
	fmt.Printf("hello world\n")
	m.Run()
}
func TestTicker(t *testing.T) {

}
