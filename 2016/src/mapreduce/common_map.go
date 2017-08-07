package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	// 1. Read content out from input file
	buf, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Printf("Fail to read from input file %s, error is %s", inFile, err)
		return
	}

	strContent := string(buf)

	// 2. Call customer map function
	keyValue := mapF(inFile, strContent)

	if len(keyValue) == 0 {
		return
	}

	// 3. Partition and write into intermedia file
	outputFileMap := make(map[uint32][]KeyValue, 0)

	for _, item := range keyValue {
		index := ihash(item.Key)
		index = index % uint32(nReduce)

		if outputFileMap[index] == nil {
			outputFileMap[index] = make([]KeyValue, 0)
		}

		outputFileMap[index] = append(outputFileMap[index], item)
	}

	for index, kv := range outputFileMap {
		writeToIntermediaFile(jobName, mapTaskNumber, index, kv)
	}
}

func writeToIntermediaFile(jobName string, // the name of the MapReduce job
	// Generate file name
	mapTaskNumber int, reduceIndex uint32, content []KeyValue) {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	index := strings.LastIndex(path, string(os.PathSeparator))
	folderPath := path[:index]
	fileName := reduceName(jobName, mapTaskNumber, int(reduceIndex))

	filePath := fmt.Sprintf("%s%s%s", folderPath, string(os.PathSeparator), fileName)

	// Encode to json file
	outputFile, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Fail to create output file %s, error is %s", filePath, err)
		return
	}
	defer outputFile.Close()

	outputEncoder := json.NewEncoder(outputFile)
	outputEncoder.Encode(content)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
