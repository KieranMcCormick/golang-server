package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

var logLocks map[int]*sync.RWMutex
var transList map[int]transaction
var fileLocks map[string]*sync.RWMutex
var createLockLock = &sync.RWMutex{}
var createFileLock = &sync.RWMutex{}

func init() {
	IP = "localhost"
	PORT = "7896"
	DIRECTORY = "./"
	TIMEOUT = 6000
	logLocks = recoverLogLocks()
	recoverCommitLogLocks()
	fileLocks = discoverFileLocks()
}

func main() {
	if len(os.Args) > 1 {
		IP = os.Args[1]
	}
	if len(os.Args) > 2 {
		PORT = os.Args[2]
	}
	if len(os.Args) > 3 {
		DIRECTORY = os.Args[3]
	}

	createDirIfNotExist(DIRECTORY)

	// checking if any lingering log files
	if len(logLocks) > 0 {
		for k := range logLocks {
			err := recoverLog(k)
			if err != nil {
				// something terrible happened
				abort(request{transactionID: k})
			}
		}
	}

	if len(transList) > 0 {
		for k := range transList {
			recoverCommitLog(k)
		}
	}

	ln, err := net.Listen("tcp", IP+":"+PORT)
	if err != nil {
		// handle error
		fmt.Println(err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println(err)
			return
		}
		go handleConnection(conn)
	}
}

// worker
func handleConnection(conn net.Conn) {
	parsePacket(conn)
	conn.Close()
}
