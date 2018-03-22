package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type request struct {
	method        string
	transactionID int
	sequenceNum   int
	contentLength int
	data          []byte
	filename      string
}

type response struct {
	method        string
	transactionID int
	sequenceNum   int
	errorCode     int
	length        int
	reason        string
}

func sendErrorIfItExist(conn net.Conn, err error) bool {
	if err != nil {
		return true
	}
	return false
}

func parseHeader(header string) (request, error) {
	method := ""
	var tid, seqNum, length int
	s := strings.Split(header, " ")
	if len(s) >= 1 {
		method = s[0]
	}
	if len(s) >= 2 {
		i, err := strconv.Atoi(s[1])
		if err != nil {
			return request{}, err
		}
		tid = i
	}
	if len(s) >= 3 {
		i, err := strconv.Atoi(s[2])
		if err != nil {
			return request{}, err
		}
		if i < 0 {
			fmt.Println("should error out")
		}
		seqNum = i
	}
	if len(s) >= 4 {
		i, err := strconv.Atoi(s[2])
		if err != nil {
			return request{}, err
		}
		length = i
	}
	// fmt.Println("header", method)
	// fmt.Println("tid", tid)
	// fmt.Println("seqnum", seqNum)
	// fmt.Println("len", length)
	return request{
		method:        method,
		transactionID: tid,
		sequenceNum:   seqNum,
		contentLength: length,
	}, nil
}

// helper to parse to stuff
func parsePacket(conn net.Conn) (request, error) {
	var req request
	var err error
	r := bufio.NewReader(conn)
	header, err := r.ReadString('\n')
	if err != nil {
		return request{}, nil
	}
	req, err = parseHeader(header)
	if err != nil {
		return request{}, nil
	}

	switch req.method {
	case "WRITE":
		req = handleWrite(req, r)
	case "READ":
		req = handleRead(req, r)
		conn.Write([]byte(string(req.data)))
	case "COMMIT":
		req = handleCommit(req)
	case "ABORT":
		req = handleAbort(req)
	default:
		req = handleError(req)
	}

	return req, nil
}

func handleWrite(req request, r *bufio.Reader) request {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		return request{}
	}
	data, err := r.ReadString('\n')
	if err != nil {
		return request{}
	}
	fmt.Println("data: ", data)
	req.data = []byte(data)
	return req
}

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

func handleRead(req request, r *bufio.Reader) request {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		return request{}
	}
	filename, err := r.ReadString('\n')
	if err != nil {
		return request{}
	}
	filename = TrimSuffix(filename, "\n")
	absPath := "./" + filename

	fmt.Println(absPath)
	req.data = readFile(absPath)
	return req
}

func handleCommit(req request) request {
	return req
}

func handleAbort(req request) request {
	return req
}

func handleError(req request) request {
	return req
}
