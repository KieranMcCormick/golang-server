package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type transaction struct {
	commitLogLock *sync.RWMutex
	isInProgress  bool
	totalNumSeq   int
}

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
func parsePacket(conn net.Conn) {
	var req request
	var err error
	r := bufio.NewReader(conn)
	header, err := r.ReadString('\n')
	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 202, "")
		sendErrorResponse(conn, res)
		return
	}
	req, err = parseHeader(header)

	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 202, "")
		sendErrorResponse(conn, res)
		return
	}

	if req.method == "WRITE" && req.sequenceNum < 1 {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 202, "")
		sendErrorResponse(conn, res)
		return
	}

	switch req.method {
	case "NEW_TXN":
		res := handleNewTransaction(req, r)
		conn.Write(constructResponse(res))
	case "WRITE":
		handleWrite(req, r, conn)
	case "READ":
		handleRead(req, r, conn)
	case "COMMIT":
		// hackjack commit before hand
		handleBeforeCommit(req, header)
		handleCommit(req, conn)
	case "ABORT":
		handleAbort(req, conn)
	default:
		sendErrorResponse(conn, response{errorCode: 202})
	}
}

func handleNewTransaction(req request, r *bufio.Reader) response {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		return newResponse("ERROR", -1, -1, 202, "")
	}
	filename, err := r.ReadString('\n')
	if err != nil {
		return newResponse("ERROR", -1, -1, 202, "")
	}
	req.filename = strings.TrimRight(filename, "\n")

	retTID := logNewTransaction(req)

	return newResponse("ACK", retTID, 0, 0, "")
}

func handleWrite(req request, r *bufio.Reader, conn net.Conn) {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 206, "")
		sendErrorResponse(conn, res)
	}
	data, err := r.ReadString('\n')
	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 206, "")
		sendErrorResponse(conn, res)
	}
	//fmt.Println("data: ", data)
	req.data = []byte(data)
	res := logWrite(req)

	// check if the transaction is in progress
	if res.method == "ERROR" {
		sendErrorResponse(conn, res)
	} else if trans, ok := transList[req.transactionID]; ok && trans.isInProgress {
		handleCommit(request{
			method:        "COMMIT",
			transactionID: req.transactionID,
			sequenceNum:   trans.totalNumSeq,
			contentLength: 0,
		}, conn)
	} else {
		sendResponse(conn, res)
	}
}

// func trimSuffix(s, suffix string) string {
// 	if strings.HasSuffix(s, suffix) {
// 		s = s[:len(s)-len(suffix)]
// 	}
// 	return s
// }

func handleRead(req request, r *bufio.Reader, conn net.Conn) {
	// reads the empty line
	_, err := r.ReadString('\n')
	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 206, "")
		sendErrorResponse(conn, res)
		return
	}
	filename, err := r.ReadString('\n')
	if err != nil {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, 206, "")
		sendErrorResponse(conn, res)
		return
	}
	filename = strings.TrimRight(filename, "\n")

	data, code := readFile(filename)
	if code != 200 {
		res := newResponse("ERROR", req.transactionID, req.sequenceNum, code, "")
		sendErrorResponse(conn, res)
	}
	conn.Write(data)
}

func handleCommit(req request, conn net.Conn) {
	res := commit(req)
	if res.method == "ERROR" {
		sendErrorResponse(conn, res)
	} else {
		sendResponse(conn, res)
	}
}

func handleAbort(req request, conn net.Conn) {
	res := abort(req)
	if res.method == "ERROR" {
		sendErrorResponse(conn, res)
	} else {
		sendResponse(conn, res)
	}
}

func sendErrorResponse(conn net.Conn, res response) {
	switch res.errorCode {
	case 201:
		res = newResponse("ERROR", res.transactionID, res.sequenceNum, 201, "Invalid transaction ID")
		sendResponse(conn, res)
	case 202:
		res = newResponse("ERROR", res.transactionID, res.sequenceNum, 202, "Invalid Invalid operation")
		sendResponse(conn, res)
	case 205:
		res = newResponse("ERROR", res.transactionID, res.sequenceNum, 205, "File I/O error")
		sendResponse(conn, res)
	case 206:
		res = newResponse("ERROR", res.transactionID, res.sequenceNum, 206, "File not found")
		sendResponse(conn, res)
	case 207:
		// Internal Error code: some seq num is missing
		sendReACK(conn, res)
	case 208:
		// Internal Error code: seq num already exist, ignore client
		return
	}
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

func sendResponse(conn net.Conn, res response) {
	conn.Write(constructResponse(res))
}

func constructResponse(res response) []byte {
	resPacket := res.method + " "
	if res.method == "ERROR" || res.method == "ACK_RESEND" {
		// error or resend
		resPacket += strconv.Itoa(res.transactionID) + " "
		resPacket += strconv.Itoa(res.sequenceNum) + " "
		resPacket += strconv.Itoa(res.errorCode) + " "
		resPacket += strconv.Itoa(len([]byte(res.reason))) + " "
		resPacket += res.reason + "\r\n\r\n"
	} else {
		// success
		resPacket += strconv.Itoa(res.transactionID) + " " // id
		resPacket += strconv.Itoa(res.sequenceNum) + " "   // seq
		resPacket += "0 0 \r\n\r\n\r\n"                    // code + length + blank
	}
	return []byte(resPacket)
}

func handleBeforeCommit(req request, header string) {
	if trans, ok := transList[req.transactionID]; ok {
		// get the lock for the commit log file
		// write the commit packet to the log file
		trans.commitLogLock.Lock()
		defer trans.commitLogLock.Unlock()

		logFileName := getCommitLogName(req.transactionID)
		appendFile(logFileName, header)
	} else {
		// fail to get the  lock for the commit log file
		// file IO error OR someone else is using it
		// fmt.Println("Couldn't get the lock")
	}
}
