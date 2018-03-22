package main

import (
	"flag"
	"fmt"
	"net"
)

func init() {
	flag.StringVar(&IP, "IP address", "127.0.0.1", "IP address")
	flag.StringVar(&PORT, "Port", "7896", "Port Number")
	flag.StringVar(&DIRECTORY, "d", "./", "Directory")
	TIMEOUT = 6000
}

func main() {
	portNum := ":" + PORT
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
