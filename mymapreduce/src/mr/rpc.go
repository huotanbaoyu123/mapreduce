package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
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
