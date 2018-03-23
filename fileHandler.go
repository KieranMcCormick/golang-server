/* build with go run *.go */

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

func discoverFileLocks() map[string]*sync.RWMutex {
	existingFileLocks := make(map[string]*sync.RWMutex)
	files, err := ioutil.ReadDir(DIRECTORY)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		//fmt.Println(f.Name())
		existingFileLocks[f.Name()] = &sync.RWMutex{}
	}
	return existingFileLocks
}

func doesFileExist(path string) bool {
	if _, err := os.Stat(path); err == nil {
		// path/to/whatever exists
		return true
	}
	return false
	// the path does not exist or some error occurred.
}

func createFile(path string) {
	createFileLock.Lock()
	defer createFileLock.Unlock()

	// detect if file exists
	var _, err = os.Stat(path)

	// create file if none exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path)
		if isError(err) {
			return
		}
		defer file.Close()
	}

	fmt.Println("file created", path)
}

func appendBytesFile(path string, message []byte) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if isError(err) {
		return
	}
	defer file.Close()
	if _, err = file.Write(message); isError(err) {
		return
	}
	if err = file.Sync(); isError(err) {
		return
	}
}

func appendFile(path, message string) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if isError(err) {
		return
	}
	defer file.Close()
	if _, err = file.WriteString(message); isError(err) {
		return
	}
	if err = file.Sync(); isError(err) {
		return
	}
}

func readFile(path string) []byte {
	// read whole file into memory from FILENAME
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(data))
	return data
}

func deleteFile(path string) {
	var err = os.Remove(path)
	if isError(err) {
		return
	}

	fmt.Println("file deleted")
}

func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}

	return (err != nil)
}
