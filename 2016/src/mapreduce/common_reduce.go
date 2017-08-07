package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	intermediaRes := make([]KeyValue, 0)
	reduceRes := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		intermediaRes = intermediaRes[:0]
		file, _ := exec.LookPath(os.Args[0])
		path, _ := filepath.Abs(file)
		index := strings.LastIndex(path, string(os.PathSeparator))
		folderPath := path[:index]
		fileName := reduceName(jobName, i, reduceTaskNumber)

		filePath := fmt.Sprintf("%s%s%s", folderPath, string(os.PathSeparator), fileName)

		intermediaFile, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("Fail to open intermedia file %s, error is %s", filePath, err)
			continue
		}
		defer intermediaFile.Close()

		intermediaDecoder := json.NewDecoder(intermediaFile)
		_, err = intermediaDecoder.Token()
		for intermediaDecoder.More() {
			var m KeyValue
			err := intermediaDecoder.Decode(&m)
			if err != nil {
				continue
			} else {
				reduceRes = append(reduceRes, m)
			}
		}
		_, err = intermediaDecoder.Token()
	}

	// 2. Sort result by key
	var sortedReduceRes KeyValueSlice
	sortedReduceRes = reduceRes[0:len(reduceRes)]
	sort.Sort(sortedReduceRes)

	// 3. Iterate items grouped by key, reduce items by calling reduceF func(key string, values []string) string
	var latestKey string
	latestValues := make([]string, 0)
	for _, item := range sortedReduceRes {
		if latestKey == "" {
			// the first item
			latestKey = item.Key
			latestValues = append(latestValues, item.Value)
		} else if latestKey != item.Key {
			// encode values in file first
			writeToOutputFile(jobName, reduceTaskNumber, latestKey, reduceF(latestKey, latestValues))

			// change latestKey to the new key and clear last values
			latestKey = item.Key
			latestValues = latestValues[:0]
		} else {
			// latestKey == item.Key, items for the same key
			latestValues = append(latestValues, item.Value)
		}
	}

	// encode last key, values to output file
	if latestKey != "" {
		writeToOutputFile(jobName, reduceTaskNumber, latestKey, reduceF(latestKey, latestValues))
	}
}

func writeToOutputFile(jobName string, reduceTaskNumber int, key, values string) {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	index := strings.LastIndex(path, string(os.PathSeparator))
	folderPath := path[:index]
	fileName := mergeName(jobName, reduceTaskNumber)

	filePath := fmt.Sprintf("%s%s%s", folderPath, string(os.PathSeparator), fileName)

	outputFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		fmt.Printf("Fail to open output file %s, error is %s", filePath, err)
		return
	}
	defer outputFile.Close()

	intermediaEncoder := json.NewEncoder(outputFile)
	intermediaEncoder.Encode(KeyValue{key, values})
}

type KeyValueSlice []KeyValue

func (kvs KeyValueSlice) Len() int {
	return len(kvs)
}

func (kvs KeyValueSlice) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs KeyValueSlice) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}
