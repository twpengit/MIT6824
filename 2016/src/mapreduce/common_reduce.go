package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// 1. Iterate all the intermedia files that from all the map tasks and belong to this reduce task
	intermediaRes := make([]KeyValue, 100)
	reduceRes := make([]KeyValue, 100)

	for i := 0; i < nMap; i++ {
		intermediaRes = intermediaRes[:0]
		file, _ := exec.LookPath(os.Args[0])
		path, _ := filepath.Abs(file)
		index := strings.LastIndex(path, string(os.PathSeparator))
		folderPath := path[:index]

		fileName := fmt.Sprintf("%s%s%s_%s_%s", folderPath, string(os.PathSeparator), jobName, i, reduceTaskNumber)

		intermediaFile, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("Fail to open intermedia file %s, error is %s", fileName, err)
			continue
		}
		defer intermediaFile.Close()

		intermediaDecoder := json.NewDecoder(intermediaFile)
		intermediaDecoder.Decode(intermediaRes)
		reduceRes = append(reduceRes, intermediaRes...)
	}

	// 2. Sort result by key

	// 3. Iterate items grouped by key, reduce items by calling reduceF func(key string, values []string) string

	// 4. Foreach key, reduced string write to output file
}
