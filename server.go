package main

import (
	"flag"
	"fmt"
	"net"
	"sync"
)

var logLocks map[int]*sync.RWMutex
var fileLocks map[string]*sync.RWMutex
var createLockLock = &sync.RWMutex{}
var createFileLock = &sync.RWMutex{}

func init() {
	flag.StringVar(&IP, "ip", "127.0.0.1", "IP address")
	flag.StringVar(&PORT, "p", "7896", "Port Number")
	flag.StringVar(&DIRECTORY, "d", "./", "Directory")
	TIMEOUT = 6000
	logLocks = recoverLogLocks()
	fileLocks = discoverFileLocks()
}

func main() {
	flag.Parse()
	portNum := ":" + PORT
	// fmt.Println(IP)
	// fmt.Println(PORT)
	// fmt.Println(DIRECTORY)
	ln, err := net.Listen("tcp", portNum)
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
