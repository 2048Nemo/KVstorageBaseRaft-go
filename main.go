package main

import (
	"KVstorageBaseRaft-go/kvserver"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	db := kvserver.NewDBServerWithConfig()
	go randOp(db)
	go randOp(db)
	err := db.Close()
	if err != nil {
		return
	}
}

func randOp(db *kvserver.KVService) {

	oplis := [4]string{"get", "put", "remove", "update"}
	keylis := [20]string{"a", "b", "c", "se", "aa", "Esx", "ky", "mio", "ss", "teio", "rs", "sada", "mc", "mq", "xxx23x", "asddaw", "xxas", "3a1sd", "liky", "fll"}
	vallis := [20]int{2, 96, -2, 5, 33, -88, 5, 23, 6, 845, 9, 2, 922, 912123, 223, 55, 31, 5223, 5, 2631}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
		op := oplis[rand.Int()%4]
		key := keylis[rand.Int()%8]
		val := vallis[rand.Int()%10]
		fmt.Println(op, " ", key, " ", val)
		if op == "get" {
			_, err := db.Get(key)
			if err != nil {
				panic(err)
			}
		} else if op == "put" {
			err := db.Put(key, int64(val))
			if err != nil {
				panic(err)
			}
		} else if op == "update" {
			err := db.Update(key, int64(val))
			if err != nil {
				panic(err)
			}
		} else {
			err := db.Remove(key)
			if err != nil {
				panic(err)
			}
		}
	}
}
