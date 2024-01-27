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
	"fmt"
	"sync"
	"sync/atomic"
)

type SharedStruct struct {
	value int64
}

func targetFunction(s SharedStruct, wg *sync.WaitGroup) {

	defer wg.Done()
	for i := 0; i < 10; i++ {
		s.value = atomic.AddInt64(&s.value, 1)
	}

}

func main() {
	var s SharedStruct
	var wg sync.WaitGroup
	wg = sync.WaitGroup{}

	wg.Add(2)

	go targetFunction(s, &wg)
	go targetFunction(s, &wg)

	wg.Wait()
	fmt.Println("Final value:", s.value)
}
