/* build with go run *.go */

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func getFullPath(filename string) string {
	absPath, err := filepath.Abs(DIRECTORY + filename)
	if err != nil {
		fmt.Println(err)
		return DIRECTORY + filename
	}
	return absPath
}

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

func doesFileExist(fileName string) bool {
	if _, err := os.Stat(getFullPath(fileName)); err == nil {
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

func createFile(fileName string) {
	createFileLock.Lock()
	defer createFileLock.Unlock()

	// detect if file exists
	var _, err = os.Stat(getFullPath(fileName))

	// create file if none exists
	if os.IsNotExist(err) {
		var file, err = os.Create(getFullPath(fileName))
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

func appendFile(fileName, message string) {
	file, err := os.OpenFile(getFullPath(fileName), os.O_APPEND|os.O_WRONLY, 0600)
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
	fmt.Println("File lock does not exist")
	return
}

func getLogFileLength(fileName string) int64 {
	data, _ := readFile(fileName)
	contents := strings.Split(string(data), "\n")
	fileToCommitName := contents[0]

	if doesFileExist(fileToCommitName) {
		file, err := os.Open(getFullPath(fileToCommitName))
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

func readFile(fileName string) ([]byte, int) {
	// read whole file into memory from FILENAME
	if lock, ok := fileLocks[fileName]; ok {
		lock.RLock()
		defer lock.RUnlock()

		data, err := ioutil.ReadFile(getFullPath(fileName))
		if err != nil {
			// ERROR: error reading file
			return []byte{}, 205
		}
		// Success
		return data, 200
	}
	// ERROR: File lock does not exist
	return []byte{}, 206
}

func deleteFile(fileName string) {
	var err = os.Remove(getFullPath(fileName))
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
