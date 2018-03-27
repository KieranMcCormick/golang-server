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
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

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
		return -1
	}
	lock.Lock()
	defer lock.Unlock()

	createFile(".log_" + strconv.Itoa(transactionID))
	appendFile(".log_"+strconv.Itoa(transactionID), r.filename)

	// Success
	return transactionID
}

func checkForSeqNum(logName string, sequenceNum int) bool {
	contents := strings.Split(string(readFile(getFullPath(logName))), "\n")
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
					return true
				}
			}
		}
	}
	return false
}

func logWrite(r request) {
	if _, ok := logLocks[r.transactionID]; ok {
		if doesFileExist(".log_" + strconv.Itoa(r.transactionID)) {
			if lock, ok := logLocks[r.transactionID]; ok {
				lock.Lock()
				defer lock.Unlock()
				if checkForSeqNum(".log_"+strconv.Itoa(r.transactionID), r.sequenceNum) {
					// ERROR: Sequence number already written to log
					return
				}

				appendFile(".log_"+strconv.Itoa(r.transactionID), "\n"+strconv.Itoa(r.sequenceNum)+" "+strconv.Itoa(r.contentLength)+"\n"+string(r.data))

				if !checkForSeqNum(".log_"+strconv.Itoa(r.transactionID), r.sequenceNum) {
					// ERROR: failed to log write
					return
				}
				//log write success
				return
			}
			// ERROR: Failed to get lock
			return
		}
		// ERROR: Transaction log does not exist
		return
	}
	// ERROR: Transaction does not exist
	return
}

//check that sequence num is good with log
func buildCommit(r request, logName string) (fileName, message string, ok bool) {
	contents := strings.Split(string(readFile(logName)), "\n")

	fileName = contents[0]
	contentArray := make([]string, r.sequenceNum)
	seqNums := make([]bool, r.sequenceNum)

	if len(contents[len(contents)-2]) > 0 { // detect if log is in middle of commit
		if strings.Split(contents[len(contents)-1], " ")[0] == "commit" {
			// ERROR: Already committing
			return "", "Already committing", false
		}
	}

	contents = contents[1:len(contents)] // bypassing file name
	currentSeqNum := 0

	numWrites := r.sequenceNum
	if numWrites == 0 {
		return fileName, "", true
	}

	flag := false
	skipFlag := false
	contentLength := 0

	for _, s := range contents {

		if flag { // loading message to memory
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
		return fileName, seqNumToReAck, false
	}
	// Success
	return fileName, strings.Join(contentArray[:], ""), true
}

func commit(r request) {
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()

		fileName, message, buildOK := buildCommit(r, ".log_"+strconv.Itoa(r.transactionID))

		if !buildOK {
			// ERROR: error passed up from buildCommit
			//fmt.Println("error: " + message)
			lock.Unlock()
			return
		}

		appendFile(".log_"+strconv.Itoa(r.transactionID), "\ncommit "+strconv.Itoa(r.sequenceNum)+" "+strconv.FormatInt(getLogFileLength(".log_"+strconv.Itoa(r.transactionID)), 10))

		if !doesFileExist(fileName) {
			createFile(fileName)
		}
		appendFile(fileName, message)

		//Need to unlock before abort so log file can be deleted
		lock.Unlock()

		//Clean up transaction
		abort(r)

		// Success
		return
	}
	// ERROR: Transaction does not exist
	return
}

func abort(r request) {
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()
		defer lock.Unlock()
		//Clean up transaction
		deleteFile(".log_" + strconv.Itoa(r.transactionID))
		delete(logLocks, r.transactionID)

		// Success
		return
	}
	// ERROR: Transaction does not exist
	return
}
