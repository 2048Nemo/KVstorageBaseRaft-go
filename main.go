//package main
//
//import (
//	"fmt"
//	"sync"
//	"sync/atomic"
//)
//
//type SharedStruct struct {
//	value int64
//}
//
//func main() {
//	var s SharedStruct
//	var wg sync.WaitGroup
//
//	wg.Add(2)
//
//	go func(s *SharedStruct) {
//		defer wg.Done()
//		for i := 0; i < 10; i++ {
//			s.value = atomic.AddInt64(&s.value, 1)
//			fmt.Println("当前的s 值为", s.value)
//		}
//		fmt.Printf("当前的s 值为%v", s.value)
//	}(&s)
//
//	go func(s *SharedStruct) {
//		defer wg.Done()
//		for i := 0; i < 10; i++ {
//			s.value = atomic.AddInt64(&s.value, -1)
//			fmt.Println("当前的s 值为", s.value)
//		}
//		fmt.Printf("当前的s 值为%v", s.value)
//	}(&s)
//
//	wg.Wait()
//	fmt.Println("Final value:", s.value)
//}
//

package main

import (
	"KVstorageBaseRaft-go/kvserver"
	"fmt"
	"time"
)

func main() {
	fmt.Println("Test Service begin")
	db := kvserver.NewDBServerWithConfig()
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
		panic("put and then get fail")
	}
	if value != 1 {
		panic("get a wrong value")
	}
	err = db.Remove("b")
	if err != nil {
		panic(err)
	}
	time.Sleep(1)
	err = db.Close()
	if err != nil {
		panic(err)
	}
}
