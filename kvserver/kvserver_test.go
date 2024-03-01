package kvserver

import (
	"fmt"
	"testing"
	"time"
)

func TestCreateServer(t *testing.T) {
	addrs := make([]string, 3)
	peers := make([]int, 3)
	addrs[0] = "127.0.0.1:10001"
	addrs[1] = "127.0.0.1:10002"
	addrs[2] = "127.0.0.1:10003"
	peers[0] = 0
	peers[0] = 1
	peers[0] = 2
	fmt.Printf("Test Create Server\n")
	servers := make([]*KVserver, 3)
	for i := 0; i < 3; i++ {
		server := NewKVServer(i, peers, addrs, "./temp")
		servers[i] = server
	}
	for i := 0; i < 3; i++ {
		servers[i].Connect()
	}
	time.Sleep(1 * time.Second)

}

func TestSend3Command(t *testing.T) {
	addrs := make([]string, 3)
	peers := make([]int, 3)
	addrs[0] = "127.0.0.1:10001"
	addrs[1] = "127.0.0.1:10002"
	addrs[2] = "127.0.0.1:10003"
	peers[0] = 0
	peers[1] = 1
	peers[2] = 2
	fmt.Printf("Test Create Server\n")
	servers := make([]*KVserver, 3)
	for i := 0; i < 3; i++ {
		server := NewKVServer(i, peers, addrs, "./temp")
		servers[i] = server

	}
	for i := 0; i < 3; i++ {
		servers[i].Connect()
	}
	time.Sleep(1 * time.Second)
	msgch := make(chan OpMsg)
	op := "put"
	key := "a"
	value := int64(1)
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		} else if servers[2].IsLeader() {
			servers[2].Exec(msgch, op, key, value)
			break
		}
	}
	outtime := time.NewTimer(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("1. receive op msg:%+v\n", msg)
	case <-outtime.C:
		t.Fatalf("Exec out of time")
	}
	op = "find"
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		} else if servers[2].IsLeader() {
			servers[2].Exec(msgch, op, key, value)
			break
		}
	}
	outtime.Reset(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("2. receive op msg:%+v\n", msg)
	case <-outtime.C:
		t.Fatalf("Exec out of time")
	}
	op = "delete"
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		} else if servers[2].IsLeader() {
			servers[2].Exec(msgch, op, key, value)
			break
		}
	}
	outtime.Reset(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("3. receive op msg:%+v\n", msg)
	case <-outtime.C:
		t.Fatalf("Exec out of time")
	}

}

func TestSendCommandSimple(t *testing.T) {
	addrs := make([]string, 2)
	peers := make([]int, 2)
	addrs[0] = "127.0.0.1:10001"
	addrs[1] = "127.0.0.1:10002"
	peers[0] = 0
	peers[1] = 1
	fmt.Printf("Test 2 Servers\n")
	servers := make([]*KVserver, 2)
	for i := 0; i < 2; i++ {
		server := NewKVServer(i, peers, addrs, "./temp")
		servers[i] = server

	}
	for i := 0; i < 2; i++ {
		servers[i].Connect()
	}
	time.Sleep(1 * time.Second)
	msgch := make(chan OpMsg)
	op := "put"
	key := "a"
	value := int64(1)
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		}
	}
	outtime := time.NewTimer(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("1. receive op msg:%+v\n", msg)
		//case <-outtime.C:
		//	t.Fatalf("Exec out of time")
	}
	op = "find"
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		}
	}
	outtime.Reset(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("2. receive op msg:%+v\n", msg)
	case <-outtime.C:
		t.Fatalf("Exec out of time")
	}
	op = "delete"
	for {
		if servers[0].IsLeader() {
			servers[0].Exec(msgch, op, key, value)
			break
		} else if servers[1].IsLeader() {
			servers[1].Exec(msgch, op, key, value)
			break
		}
	}
	outtime.Reset(2 * time.Second)
	select {
	case msg := <-msgch:
		if !msg.Succ {
			t.Fatalf("Fail to apply. Err msg: %v", msg.Msg)
		}
		fmt.Printf("3. receive op msg:%+v\n", msg)
	case <-outtime.C:
		t.Fatalf("Exec out of time")
	}

}

func TestServiceGet(t *testing.T) {
	fmt.Println("Test Service begin")
	db := NewDBServerWithConfig()
	err := db.Put("a", 1)
	if err != nil {
		fmt.Printf("put数据错误%v\n", err)
	}
	err = db.Put("b", 1)
	if err != nil {
		fmt.Printf("put数据错误%v\n", err)
	}
	err = db.Put("abc", 1)
	if err != nil {
		fmt.Printf("put数据错误%v\n", err)
	}
	value, err := db.Get("a")
	if err != nil {
		t.Fatal("put and then get fail")
	}
	if value != 1 {
		t.Fatal("get a wrong value")
	}
	db.Remove("b")
	db.Close()
}
