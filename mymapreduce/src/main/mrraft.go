package main

import (
	"../raft"
)
//import "math/rand"
//import "sync/atomic"
//import "sync"
// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).


//we need a main thread fo use to debug
func main() {


	raft.RunMaintest()

}