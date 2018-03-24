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
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func recoverLogLocks() map[int]*sync.RWMutex {
	existingLogLocks := make(map[int]*sync.RWMutex)

	files, err := ioutil.ReadDir(DIRECTORY)
	if err != nil {
		log.Fatal(err)
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
		fmt.Println("Failed to get lock")
		return -1
	}
	lock.Lock()
	defer lock.Unlock()
	createFile(DIRECTORY + ".log_" + strconv.Itoa(transactionID))
	appendFile(DIRECTORY+".log_"+strconv.Itoa(transactionID), r.filename)
	return transactionID
}

/* 	if _, ok := logLocks[r.transactionID]; !ok {
//fmt.Println(strconv.Itoa(r.transactionID))
if !doesFileExist(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID)) {
*/
//Failed to get lock
//Return
/* 		} else {
		//Log was not deleted properly
		fmt.Println("Log was not deleted properly")
		return -1
	}
} else {
	//Transaction is already open
	fmt.Println("Transaction is already open")
	return -1
} */

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

//check that sequence num is good with log
func buildCommit(r request, path string) (fileName, message string, ok bool) {
	contents := strings.Split(string(readFile(path)), "\n")

	fileName = contents[0]
	contentArray := make([]string, r.sequenceNum)
	fmt.Println("last: " + contents[len(contents)-1])

	if len(contents[len(contents)-2]) > 0 { // detect if code is in middle of commit
		fmt.Println("second last: " + contents[len(contents)-2])
		fmt.Println(strings.Split(contents[len(contents)-2], " ")[0])
		if strings.Split(contents[len(contents)-2], " ")[0] == "commit" {
			//Already committing
			return "", "Already committing", false
		}
	}
	contents = contents[1 : len(contents)-1] // bypassing file name and commit message
	currentSeqNum := 0
	numWrites := r.sequenceNum

	flag := false
	contentLength := 0

	for _, s := range contents {
		if flag { // loading message to memory
			if s == "" {
				contentArray[currentSeqNum] = contentArray[currentSeqNum] + "\n"
				contentLength--
				if contentLength == 0 {
					numWrites--
					flag = false
				}
			} else {
				contentArray[currentSeqNum] = (contentArray[currentSeqNum] + s)
				contentLength = contentLength - len(s)
				if contentLength == 0 {
					numWrites--
					flag = false
				}
			}
		} else { // Line is seq num and data len
			if s != "" {
				logLine := strings.Split(string(s), " ")
				/* fmt.Print("logLine: ")
				fmt.Println(logLine) */

				currentSeqNum, _ = strconv.Atoi(string(logLine[0]))
				contentLength, _ = strconv.Atoi(string(logLine[1]))
				flag = true
			}
		}
	}
	return fileName, strings.Join(contentArray[:], ""), true
}

func commit(r request) {
	if _, ok := logLocks[r.transactionID]; ok {
		appendFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), "\ncommit "+strconv.Itoa(r.sequenceNum))
		fileName, message, buildOK := buildCommit(r, DIRECTORY+".log_"+strconv.Itoa(r.transactionID))
		fmt.Println("fin build")

		if !buildOK {
			//error from buildCommit
			fmt.Println("error: " + message)

			return
		}
		if !doesFileExist(DIRECTORY + fileName) {
			createFile(DIRECTORY + fileName)
		}
		appendFile(DIRECTORY+fileName, message)

		//Clean up transaction
		deleteFile(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID))
		delete(logLocks, r.transactionID)

		return
	}
	//Transaction does not exist
	fmt.Println("error")

	return
}

/*
func logStartCommit(r request) {
	if lock, ok := logLocks[r.transactionID]; ok {
		lock.Lock()
		defer lock.Unlock()
		buildCommit(r)
		fmt.Println("Got lock")

		fmt.Println("Starting commit")
		return
	}
	fmt.Println("failed to get lock")
} */
