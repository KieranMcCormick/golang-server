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

func newResponse(method string, id, seq, code int, reason string) response {
	return response{
		method:        method,
		transactionID: id,
		sequenceNum:   seq,
		errorCode:     code,
		length:        len([]byte(reason)),
		reason:        reason,
	}
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
		lenStr := strings.TrimRight(s[3], "\r\n")
		i, err := strconv.Atoi(lenStr)
		if err != nil {
			fmt.Println(err)
			return request{}, err
		}
		length = i
	}
	return request{
		method:        method,
		transactionID: tid,
		sequenceNum:   seqNum,
		contentLength: length,
	}, nil
}

// helper to parse to stuff
func parsePacket(conn net.Conn) error {
	var req request
	var err error
	r := bufio.NewReader(conn)
	header, err := r.ReadString('\n')
	if err != nil {
		return nil
	}
	req, err = parseHeader(header)

	if err != nil {
		return nil
	}

	switch req.method {
	case "NEW_TXN":
		tid := strconv.Itoa(handleNewTransaction(req, r))
		conn.Write([]byte("ACK " + tid))
	case "WRITE":
		req = handleWrite(req, r)
	case "READ":
		req = handleRead(req, r)
		conn.Write(req.data)
	case "COMMIT":
		handleCommit(req, conn)
	case "ABORT":
		req = handleAbort(req)
	default:
		req = handleError(req)
	}

	return nil
}

func handleNewTransaction(req request, r *bufio.Reader) int {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		return -1
	}
	filename, err := r.ReadString('\n')
	if err != nil {
		return -1
	}
	req.filename = strings.TrimRight(filename, "\n")

	retTID := logNewTransaction(req)

	return retTID
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
	//fmt.Println("data: ", data)
	req.data = []byte(data)
	logWrite(req)

	return req
}

// func trimSuffix(s, suffix string) string {
// 	if strings.HasSuffix(s, suffix) {
// 		s = s[:len(s)-len(suffix)]
// 	}
// 	return s
// }

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
	filename = strings.TrimRight(filename, "\n")

	//fmt.Println(absPath)
	req.data = readFile(filename)
	return req
}

func handleCommit(req request, conn net.Conn) {
	res := commit(req)
	if res.errorCode == 207 {
		sendReACK(conn, res)
	} else {
		sendResponse(conn, res)
	}
}

func handleAbort(req request) request {
	abort(req)
	return req
}

func handleError(req request) request {
	return req
}

func sendResponse(conn net.Conn, res response) {
	conn.Write(constructResponse(res))
}

func constructResponse(res response) []byte {
	resPacket := res.method + " "
	if res.errorCode == 200 {
		// success
		resPacket += strconv.Itoa(res.transactionID) + " " // id
		resPacket += strconv.Itoa(res.sequenceNum) + " "   // seq
		resPacket += "0 "                                  // code
		resPacket += "0 "                                  // length
		resPacket += "\r\n\r\n\r\n"
	} else {
		// error
		resPacket += strconv.Itoa(res.transactionID) + " "
		resPacket += strconv.Itoa(res.sequenceNum) + " "
		resPacket += strconv.Itoa(res.errorCode) + " "
		resPacket += strconv.Itoa(len([]byte(res.reason))) + " "
		resPacket += res.reason + "\r\n\r\n"
	}
	return []byte(resPacket)
}

func sendReACK(conn net.Conn, res response) {
	seqNum := strings.Split(res.reason, " ")
	for i := 0; i < len(seqNum); i++ {
		reason := "Missing sequence number " + seqNum[i]
		seqNum, err := strconv.Atoi(seqNum[i])
		if err != nil {
			seqNum = -1
		}
		ackRes := newResponse("ERROR", res.transactionID, seqNum, 207, reason)
		sendResponse(conn, ackRes)
	}
}
