package main

import (
	"fmt"
	"net"
)

func main() {
	const TimeOut = 6000
	const Port = ":7896"

	ln, err := net.Listen("tcp", Port)
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
