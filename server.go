package main

import (
	"bufio"
	"fmt"
	"net"
)

type request struct {
	method      string
	transtionID int
	sequenceNum int
	data        []byte
	filename    string
}

type response struct {
	method      string
	transtionID int
	sequenceNum int
	errorCode   int
	length      int
	reason      string
}

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

// helper to parse to stuff
// returns METHOD, TRANSACTION NUMBER,
func parsePacket(conn net.Conn) request {
	var s string
	r := bufio.NewReader(conn)
	for {
		message, err := r.ReadString('\n')
		if err != nil {
			break
		}
		s += string(message)
	}
	fmt.Println(s)
	return request{
		method:      "WRITE",
		transtionID: 123,
		sequenceNum: 1,
	}
}
