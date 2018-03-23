package main

/* Log file structure
** first line name of file
** writes are a line with a number indicating the sequence
** followed by a byte array
**
** commits:
**
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
		fmt.Println(strconv.Itoa(r.transactionID))

		if !doesFileExist(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID)) {
			logLocks[r.transactionID] = &sync.RWMutex{}
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

func logWrite(r request) {

	if _, ok := logLocks[r.transactionID]; ok {
		if doesFileExist(DIRECTORY + ".log_" + strconv.Itoa(r.transactionID)) {
			if lock, ok := logLocks[r.transactionID]; ok {
				lock.Lock()
				defer lock.Unlock()
				appendFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), "\n"+strconv.Itoa(r.transactionID)+"\n")
				appendBytesFile(DIRECTORY+".log_"+strconv.Itoa(r.transactionID), r.data)
				return
			}
			//Failed to get lock
			//Return
		} else {
			//Transaction log does not exist
			return
		}
	} else {
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
