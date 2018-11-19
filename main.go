package main

import (
	"fmt"
	"github.com/sysco-middleware/dup-backend-processor/internal/errors"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"time"
)

// todo : replace with environment vars
const (
	sourceDir = "/home/prakhar/dup_input"
	backupDir = "/home/prakhar/dup_input/backup"
	tempDir   = "/home/prakhar/dup_input/temp"
)

func main() {
	// Create required channels
	fileChan := make(chan os.FileInfo)
	errrChan := make(chan *errors.FileError)
	tempChan := make(chan string)

	//go DupFileParser(fileChan, errrChan)

	for {

		fmt.Println("before", runtime.NumGoroutine())
		select {
		case <-time.Tick(time.Second * 5):
			go DupFilePoller(fileChan, errrChan)

		case fileInfo := <-fileChan:
			go DupFileBackup(fileInfo, errrChan, tempChan)
			fmt.Println(fileInfo)

		case err := <-errrChan:
			fmt.Println(err.Details)
			fmt.Println(err.Code)
			fmt.Println(err.Message)

		case fileLocation := <-tempChan:
			go DupFileParser(fileLocation, errrChan)
		}
		fmt.Println("after", runtime.NumGoroutine())
	}

}

// DupFilePoller gets a new tick every 10 seconds and starts polling the directory to find new files
func DupFilePoller(fileChan chan<- os.FileInfo, errChan chan<- *errors.FileError) {
	t1 := time.Now()
	fmt.Println("Polling source directory")

	// Read files from the source directory
	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		// in case of file read error, send message on error channel
		errChan <- errors.NewFileError(err.Error(), 0, "Could not read from source dir")
		return
	}

	for _, file := range files {
		if !file.IsDir() {
			fileChan <- file
		}
	}
	fmt.Println(time.Now().Sub(t1))
}

func DupFileParser(fileParserChannel string, errChan chan<- *errors.FileError) {
	//for v := range fileParserChannel {
	//fmt.Println("from the file parser channel")
	//fmt.Print(v)
	fmt.Print(fileParserChannel)
	if fileParserChannel != "" {
		// read the file
		fileNameParts := strings.Split(fileParserChannel, ".")
		fileNamePartsLen := len(fileNameParts)
		if fileNamePartsLen == 0 {
			errChan <- errors.NewFileError("File Parsing Error", 10, "Invalid Name")
			return
		}

		fileNameExtension := fileNameParts[fileNamePartsLen-1]
		if "csv" != strings.ToLower(fileNameExtension) {
			errChan <- errors.NewFileError("File Parsing Error", 11, "Invalid file extension")
			return
		}

		file, err := os.Open(fileParserChannel)
		if err != nil {
			errChan <- errors.NewFileError("File Parsing Error", 12, err.Error())
			return
		}

		fileInfo, err := file.Stat()
		if err != nil {
			errChan <- errors.NewFileError("File Parsing Error", 13, err.Error())
			return
		}

		if fileInfo.Size() > 10000000 {
			errChan <- errors.NewFileError("File Parsing Error", 14, "File too big")
		}
	}
	//}
}

// parses a single file
// Get the file details
// move the file to the temp directory
// read the contents of the file line by line
func DupFileBackup(file os.FileInfo, errorChan chan<- *errors.FileError, ch chan<- string) {
	var data []byte
	var err error
	prefix := time.Now().Format("20060102150405")
	sourceLocation := fmt.Sprintf("%s/%s", sourceDir, file.Name())
	backupLocation := fmt.Sprintf("%s/%s_%s", backupDir, prefix, file.Name())
	tempLocation := fmt.Sprintf("%s/%s_%s", tempDir, prefix, file.Name())
	//fileModeReadOnly := os.FileMode(os.O_RDONLY)
	//fileModeReadWrite := os.FileMode(os.O_CREATE)

	// 0. Read the content of the source file
	if data, err = ioutil.ReadFile(sourceLocation); err != nil {
		errorChan <- errors.NewFileError(err.Error(), 0, sourceLocation)
	}

	// 1. Archive file to backup directory
	if err = ioutil.WriteFile(backupLocation, data, 0644); err != nil {
		errorChan <- errors.NewFileError(err.Error(), 1, backupLocation)
	}

	// 2. Make a copy in the temp directory
	if err = ioutil.WriteFile(tempLocation, data, 0644); err != nil {
		errorChan <- errors.NewFileError(err.Error(), 2, tempLocation)
	}

	// 3. Start processing the file in the temp_directory
	// send to channel with temp file location
	if err = os.Remove(sourceLocation); err != nil {
		errorChan <- errors.NewFileError(err.Error(), 3, sourceLocation)
	}

	ch <- tempLocation
}
