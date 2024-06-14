package mr

import (
	"fmt"
	"io"
	"log"
	"os"
)

const OUTPUT_DIR = "mr-out"
const TEMP_DIR_NAME = "mr-tmp"
const TEMP_FILE_PREFIX = "mr"
const TEMP_FILE_FORMAT = TEMP_FILE_PREFIX + "-%v-%v"

func Read(filename string) string {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func GetIntermediateFileName(mapWorkerId int, reduceTaskId int) string {
	return fmt.Sprintf(TEMP_FILE_FORMAT, mapWorkerId, reduceTaskId)
}

func GetIntermediateFileNamesByMapTaskId(mapWorkerId int, nReduce int) []string {
	intermediateFilenames := make([]string, nReduce)
	for r := 0; r < nReduce; r++ {
		intermediateFilenames[r] = GetIntermediateFileName(mapWorkerId, r)
	}
	return intermediateFilenames
}

func GetIntermediateFileNamesByReduceTaskId(nMap int, reduceTaskId int) []string {
	intermediateFiles := make([]string, nMap)
	for m := 0; m < nMap; m++ {
		intermediateFiles[m] = GetIntermediateFileName(m, reduceTaskId)
	}
	return intermediateFiles
}

func CreateIntermediateFiles(workerId int, nReduce int) []*os.File {
	// Make temporary kvs data directory
	if _, err := os.Stat(TEMP_DIR_NAME); os.IsNotExist(err) {
		os.Mkdir(TEMP_DIR_NAME, os.ModeDir|0755)
	}
	//else if !os.IsExist(err) {
	//	log.Fatalf("cannot make temporary kvs data directory: %v", tmpDir)
	//}

	intermediateFiles := make([]*os.File, nReduce)
	intermediateFilenames := GetIntermediateFileNamesByMapTaskId(workerId, nReduce)

	for k, f := range intermediateFilenames {

		path := TEMP_DIR_NAME + "/" + f
		ofile, err := os.Create(path)
		if err != nil {
			log.Fatalf("cannot create: %v", path)
		}

		intermediateFiles[k] = ofile
	}

	return intermediateFiles
}
