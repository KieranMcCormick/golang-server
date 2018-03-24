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
** log that commit is starting
** builds commit from log
** creates file if none exists
** appends write data to file
**
*/

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

func recoverLogLocks() map[int]*sync.RWMutex {
	return make(map[int]*sync.RWMutex)
}

func logNewTransaction(r request) {
	if _, ok := logLocks[r.transactionID]; !ok {
		//fmt.Println(strconv.Itoa(r.transactionID))

		if !doesFileExist(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID)) {
			logLocks[r.transactionID] = &sync.RWMutex{}
			for _, lock := range logLocks {
				fmt.Print("lock: ")
				fmt.Println(lock)
			}
			if lock, ok := logLocks[r.transactionID]; ok {
				lock.Lock()
				defer lock.Unlock()
				createFile(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID))
				appendFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), r.filename)
				return
			}
			//Failed to get lock
			fmt.Println("Failed to get lock")
			//Return
		} else {
			//Log was not deleted properly
			fmt.Println("Log was not deleted properly")
			return
		}
	} else {
		//Transaction is already open
		fmt.Println("Transaction is already open")
		return
	}
}

func checkForSeqNum(path string, sequenceNum int) bool {
	contents := strings.Split(string(readFile(path)), "\n")
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
				/* 				fmt.Print("logLine: ")
				   				fmt.Println(logLine) */
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
		if doesFileExist(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID)) {
			if lock, ok := logLocks[r.transactionID]; ok {
				lock.Lock()
				defer lock.Unlock()
				if checkForSeqNum(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), r.sequenceNum) {
					//Sequence number already written to log
					fmt.Println("Sequence number already written to log")

					return
				}

				//log write
				appendFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), "\n"+strconv.Itoa(r.sequenceNum)+" "+strconv.Itoa(r.contentLength)+"\n"+string(r.data))

				if !checkForSeqNum(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), r.sequenceNum) {
					//failed to log write
					fmt.Println("failed to log write")

					return
				}
				//log write success
				fmt.Println("log write success")
				return
			}
			//Failed to get lock
			fmt.Println("Failed to get lock")

			//Return
		} else {
			fmt.Println("Transaction log does not exist")
			//Transaction log does not exist
			return
		}
	} else {
		fmt.Println("Transaction does not exist")
		//Transaction does not exist
		return
	}
}

func buildCommit(r request) {
	logContents := strings.Split(string(readFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID))), `\n`)
	for _, log := range logContents {
		fmt.Println(log)

	}
}

func logStartCommit(r request) {
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()
		defer lock.Unlock()

		fmt.Println("Got lock")

		fmt.Println("Starting commit")
		return
	}
	fmt.Println("failed to get lock")
}
