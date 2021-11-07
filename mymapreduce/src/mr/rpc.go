package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strings"
	"unicode"
)
import "strconv"





//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TryMapArgs struct {
}

type TryMapReply struct {
	// if should not run map, run reduce
	RunMap bool
}

const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type TaskInfo struct {
	/*
		Declared in consts above
			0  map
			1  reduce
			2  wait
			3  end
	*/
	State int

	FileName  string
	FileIndex int
	PartIndex int

	NReduce int
	NFiles  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())

	if isLinux!=true{
		s =":2234"
	}

	return s
}

func masterNetwork() string {
	s :="unix"
	if isLinux!=true{
		s ="tcp"
	}
    return s
}
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}