package main

import (
	"bufio"
	"fmt"
	"log"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	//	"time"
)

const (
	nNumber = 1000 //00
	nMap    = 100
	nReduce = 50
)

func MapFunc(file string, value string) (res []mapreduce.KeyValue) {
	//	debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := mapreduce.KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// Just return key
func ReduceFunc(key string, values []string) string {
	//	for _, e := range values {
	//		debug("Reduce %s %v\n", key, e)
	//}
	return ""
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *mapreduce.Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := mapreduce.Distributed("test", files, nReduce, master)
	return mr
}

func cleanup(mr *mapreduce.Master) {
	mr.CleanupFiles()
	for _, f := range mr.Files() {
		os.Remove(f)
	}
}

func main() {
	mr := mapreduce.Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(nil, mr.Files())
	checkWorker(nil, mr.Stats())
	cleanup(mr)
}
