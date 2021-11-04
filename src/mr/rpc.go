package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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
type TaskInfo struct {
	/*** state value
	  * 0 --> map
	    1 --> reduce
	    2 --> wait
	    3 --> nothing to do
	*/
	State        int
	//要读取的文件名
	FileName     string
	//经过map后输出到哪个file中-->针对map
	FileIdx      int
	//要写到哪个文件-->针对reduce
	OutFileIdx   int
	//分成几个reduce
	ReduceNum    int
	FileNum      int
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
