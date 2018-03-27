/* build with go run *.go */

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

func discoverFileLocks() map[string]*sync.RWMutex {
	existingFileLocks := make(map[string]*sync.RWMutex)
	files, err := ioutil.ReadDir(DIRECTORY)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		existingFileLocks[f.Name()] = &sync.RWMutex{}
	}

	return existingFileLocks
}

func doesFileExist(path, fileName string) bool {
	if _, err := os.Stat(path + fileName); err == nil {
		//If file exists but no lock exists for that file, then create the lock
		if _, ok := fileLocks[fileName]; !ok {
			fileLocks[fileName] = &sync.RWMutex{}
		}

		// Success
		return true
	}
	// The path does not exist or some error occurred.
	return false
}

func createFile(path, fileName string) {
	createFileLock.Lock()
	defer createFileLock.Unlock()

	// detect if file exists
	var _, err = os.Stat(path + fileName)

	// create file if none exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path + fileName)
		if isError(err) {
			// ERROR: Error creating file
			return
		}
		defer file.Close()
		fileLocks[file.Name()] = &sync.RWMutex{}
	}
	// Success
}

func appendBytesFile(path, fileName string, message []byte) {
	file, err := os.OpenFile(path+fileName, os.O_APPEND|os.O_WRONLY, 0600)
	if isError(err) {
		// ERROR: error opening file
		return
	}
	defer file.Close()
	if lock, ok := fileLocks[file.Name()]; ok {
		lock.Lock()
		defer lock.Unlock()

		if _, err = file.Write(message); isError(err) {
			// ERROR: error writing to file
			return
		}
		if err = file.Sync(); isError(err) {
			// ERROR: Error syncing file
			return
		}
		//Success
		return
	}
	// Error: File lock does not exist
	return
}

func appendFile(path, fileName, message string) {
	file, err := os.OpenFile(path+fileName, os.O_APPEND|os.O_WRONLY, 0600)
	if isError(err) {
		return
	}
	defer file.Close()

	if lock, ok := fileLocks[file.Name()]; ok {
		lock.Lock()
		defer lock.Unlock()

		if _, err = file.WriteString(message); isError(err) {
			// ERROR: Error writing to file
			return
		}
		if err = file.Sync(); isError(err) {
			// ERROR: Error syncing file
			return
		}
		//Success
		return
	}
	// Error: File lock does not exist
	return
}

func getLogFileLength(path, fileName string) int64 {

	contents := strings.Split(string(readFile(path, fileName)), "\n")
	fileToCommitName := contents[0]

	if doesFileExist(DIRECTORY, fileToCommitName) {
		file, err := os.Open(DIRECTORY + fileToCommitName)
		defer file.Close()

		if lock, ok := fileLocks[file.Name()]; ok {
			lock.RLock()
			defer lock.RUnlock()

			if isError(err) {
				// ERROR: Could not open file, IO error
				return 0
			}

			fi, err := file.Stat()
			if err != nil {
				// ERROR: Could not obtain stat, handle error
				return 0
			}
			// Success
			return fi.Size()
		}
		// ERROR: File lock does not exist
		return 0
	}
	// Success
	return 0
}

func readFile(path, fileName string) []byte {
	// read whole file into memory from FILENAME
	if lock, ok := fileLocks[fileName]; ok {
		lock.RLock()
		defer lock.RUnlock()

		data, err := ioutil.ReadFile(path + fileName)
		if err != nil {
			// ERROR: error reading file
			return []byte{}
		}
		// Success
		return data
	}
	// ERROR: File lock does not exist
	return []byte{}
}

func deleteFile(path, fileName string) {

	var err = os.Remove(path + fileName)
	if isError(err) {
		// ERROR: error deleting file
		return
	}
	delete(fileLocks, fileName)
	// Success
	return

}

func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}

	return (err != nil)
}
