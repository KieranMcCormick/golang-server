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
			if contentLength != 0 {
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
			} else {
				flag = false
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

/*
func checkForLine(path string) {
	f, err := os.Open(path)
	if err != nil {
		//return 0, err
		return
	}
	defer f.Close()

	// Splits on newlines by default.
	scanner := bufio.NewScanner(f)

	line := 1
	// https://golang.org/pkg/bufio/#Scanner.Scan
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "1") {
			fmt.Print("line: ")
			fmt.Println(line)
			//return line, nil
			return
		}

		line++
	}

	if err := scanner.Err(); err != nil {
		// Handle the error
	}
} */

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
