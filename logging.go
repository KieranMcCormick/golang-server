package main

/* Log file structure
** first line name of file to write to
** writes are a line with the sequence num followed by data len
** followed by data
** i.e.

	test_file.txt
	0 44
	WRITE 1 0 0 Sat, 24 Mar 2018 12:01:10 -0700

	1 44
	WRITE 1 1 0 Sat, 24 Mar 2018 12:01:13 -0700

	5 44
	WRITE 1 5 0 Sat, 24 Mar 2018 12:01:19 -0700

	4 44
	WRITE 1 4 0 Sat, 24 Mar 2018 12:01:22 -0700



** commits:
** check for transaction log
** log that commit is starting
** builds commit from log
** creates file if none exists
** appends write data to file
**
*/

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func getLogName(id int) string {
	return ".log_" + strconv.Itoa(id)
}

func recoverLogLocks() map[int]*sync.RWMutex {
	existingLogLocks := make(map[int]*sync.RWMutex)

	files, err := ioutil.ReadDir(DIRECTORY)
	if err != nil {
		// ERROR: error reading directory
		return existingLogLocks
	}

	for _, f := range files {
		r, err := regexp.MatchString(".log_", f.Name())
		if err == nil && r {
			tid, _ := strconv.Atoi(f.Name()[5:])
			existingLogLocks[tid] = &sync.RWMutex{}
		}
	}
	return existingLogLocks
}

func getNewTransactionID() int {
	createLockLock.Lock()
	defer createLockLock.Unlock()

	i := 0
	if len(logLocks) > 0 {
		for k := range logLocks {
			if k != i {
				logLocks[i] = &sync.RWMutex{}
				return i
			}
			i++
		}
	}
	logLocks[i] = &sync.RWMutex{}
	return i
}

func logNewTransaction(r request) int {
	transactionID := getNewTransactionID()
	lock, ok := logLocks[transactionID]
	if !ok {
		// ERROR: Failed to get lock, server error
		// TODO:? would logLocks has index error?
		return -1
	}
	lock.Lock()
	defer lock.Unlock()

	logFileName := getLogName(transactionID)
	createFile(logFileName)
	appendFile(logFileName, r.filename)

	// Success
	return transactionID
}

func checkIfSeqExist(logName string, sequenceNum int) int {
	data, code := readFile(logName)
	if code != 200 {
		return code
	}
	contents := strings.Split(string(data), "\n")
	contents = contents[1:len(contents)] // bypassing file name
	flag := false
	contentLength := 0
	for _, s := range contents {
		if flag { // bypassing message
			if s != "" {
				contentLength = contentLength - len(s)
				if contentLength == 0 {
					flag = false
				}
			} else {
				contentLength--
				if contentLength == 0 {
					flag = false
				}
			}
		} else { // Line is seq num and data len
			if s != "" {
				logLine := strings.Split(string(s), " ")
				lineSeqNum, _ := strconv.Atoi(string(logLine[0]))
				if lineSeqNum != sequenceNum {
					flag = true
					contentLength, _ = strconv.Atoi(string(logLine[1]))
				} else {
					return 208
				}
			}
		}
	}
	// ERROR: what's here
	return 200
}

func logWrite(r request) response {
	logFileName := getLogName(r.transactionID)
	if _, ok := logLocks[r.transactionID]; ok {
		if doesFileExist(logFileName) {
			if lock, ok := logLocks[r.transactionID]; ok {
				lock.Lock()
				defer lock.Unlock()
				checkSeqStatus := checkIfSeqExist(logFileName, r.sequenceNum)
				if checkSeqStatus == 208 {
					// ERROR: Sequence number already written to log
					fmt.Println("some error: ", checkSeqStatus)
					return newResponse("ERROR", r.transactionID, r.sequenceNum, checkSeqStatus, "")
				}

				appendFile(logFileName, "\n"+strconv.Itoa(r.sequenceNum)+" "+strconv.Itoa(r.contentLength)+"\n"+string(r.data))

				checkSeqStatus = checkIfSeqExist(logFileName, r.sequenceNum)
				if checkSeqStatus == 208 {
					//log write success
					return newResponse("SUCCSS", r.transactionID, r.sequenceNum, 200, "")
				}

				// ERROR: failed to log write
				fmt.Println("failed to log write", checkSeqStatus)
				return newResponse("ERROR", r.transactionID, r.sequenceNum, checkSeqStatus, "")
			}
			// ERROR: Failed to get lock
			// TODO:?
			return newResponse("ERROR", r.transactionID, r.sequenceNum, 205, "")
		}
		// ERROR: Transaction log does not exist
		// TODO:?
		return newResponse("ERROR", r.transactionID, r.sequenceNum, 206, "")
	}
	// ERROR: Transaction does not exist
	return newResponse("ERROR", r.transactionID, r.sequenceNum, 201, "")
}

//check that sequence num is good with log
func buildCommit(r request, logFileName string) (fileName, message string, code int) {
	data, _ := readFile(logFileName)
	contents := strings.Split(string(data), "\n")

	fileName = contents[0]
	contentArray := make([]string, r.sequenceNum)
	seqNums := make([]bool, r.sequenceNum)

	// breaks when no write is in there
	// if len(contents[len(contents)-2]) > 0 { // detect if log is in middle of commit
	// 	if strings.Split(contents[len(contents)-1], " ")[0] == "commit" {
	// 		// ERROR: Already committing
	// 		return "", "Already committing", 300
	// 	}
	// }

	contents = contents[1:len(contents)] // bypassing file name
	currentSeqNum := 0

	numWrites := r.sequenceNum
	if numWrites == 0 {
		return fileName, "", 200
	}

	// indicates if it's parsing content or not --> loading message to memory
	flag := false
	skipFlag := false
	contentLength := 0

	for _, s := range contents {
		if flag {
			if s == "" {
				if !skipFlag {
					contentArray[currentSeqNum] = contentArray[currentSeqNum] + "\n"
				}
				contentLength--
				if contentLength == 0 {
					if !skipFlag {
						numWrites--
					}
					flag = false
					skipFlag = false
				}
			} else {
				if !skipFlag {
					contentArray[currentSeqNum] = (contentArray[currentSeqNum] + s)
				}
				contentLength = contentLength - len(s)
				if contentLength == 0 {
					if !skipFlag {
						numWrites--
					}
					flag = false
					skipFlag = false
				}
			}
		} else { // Line is seq num and data len
			if s != "" {
				if numWrites == 0 {
					break
				}
				logLine := strings.Split(string(s), " ")

				currentSeqNum, _ = strconv.Atoi(string(logLine[0]))

				if currentSeqNum >= r.sequenceNum { //
					skipFlag = true
				} else {
					seqNums[currentSeqNum] = true
				}

				contentLength, _ = strconv.Atoi(string(logLine[1]))
				flag = true
			}
		}
	}

	allSeqNums := true
	seqNumToReAck := ""
	for i, t := range seqNums {
		if t == false {
			allSeqNums = false
			if len(seqNumToReAck) != 0 {
				seqNumToReAck = seqNumToReAck + " "
			}
			seqNumToReAck = seqNumToReAck + strconv.Itoa(i)
		}
	}
	if !allSeqNums {
		// ERROR: Not all sequence numbers written
		return fileName, seqNumToReAck, 207
	}
	// Success
	return fileName, strings.Join(contentArray[:], ""), 200
}

func commit(r request) response {
	logFileName := getLogName(r.transactionID)
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()

		fileName, message, code := buildCommit(r, logFileName)

		if code != 200 {
			// ERROR: error passed up from buildCommit
			//fmt.Println("error: " + message)
			lock.Unlock()
			return newResponse("ERROR", r.transactionID, r.sequenceNum, code, message)
		}

		appendFile(logFileName, "\ncommit "+strconv.Itoa(r.sequenceNum)+" "+strconv.FormatInt(getLogFileLength(logFileName), 10))

		if !doesFileExist(fileName) {
			createFile(fileName)
		}
		appendFile(fileName, message)

		//Need to unlock before abort so log file can be deleted
		lock.Unlock()

		//Clean up transaction
		res := abort(r)

		// Success or Fail
		return res
	}
	// ERROR: Transaction does not exist
	return newResponse("ERROR", r.transactionID, r.sequenceNum, 201, "")
}

func abort(r request) response {
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()
		defer lock.Unlock()
		//Clean up transaction
		deleteFile(getLogName(r.transactionID))
		delete(logLocks, r.transactionID)

		// Success
		return newResponse("SUCCESS", r.transactionID, r.sequenceNum, 200, "")
	}
	// ERROR: Transaction does not exist
	return newResponse("ERROR", r.transactionID, r.sequenceNum, 201, "")
}
