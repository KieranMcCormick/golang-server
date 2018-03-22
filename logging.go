package main

import (
	"fmt"
	"net"
	"sync"
)

var logLocks map[string]sync.RWMutex

func logNewTransaction(file string) {
	if lock, ok := logLocks[file]; ok {
		fmt.Println("Got lock")
		lock.RLock()
		defer lock.RUnlock()
		fmt.Println("New Transaction")
	}
	fmt.Println("failed to get lock")
}

func logWrite(file string, conn net.Conn) {
	if lock, ok := logLocks[file]; ok {
		fmt.Println("Got lock")
		lock.RLock()
		defer lock.RUnlock()
		fmt.Println("Write complete")
	}
	fmt.Println("failed to get lock")
}

func logStartCommit(file string) {
	if lock, ok := logLocks[file]; ok {
		fmt.Println("Got lock")
		lock.RLock()
		defer lock.RUnlock()
		fmt.Println("Starting commit")
	}
	fmt.Println("failed to get lock")
}
