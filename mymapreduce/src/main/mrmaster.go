package main

import "../mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	var m *mr.Coordinator

    if mr.IsLinux!=true{
		files:=[] string{"src/main/pg-being_ernest.txt","src/main/pg-dorian_gray.txt",
			"src/main/pg-frankenstein.txt","src/main/pg-grimm.txt",
			"src/main/pg-huckleberry_finn.txt","src/main/pg-metamorphosis.txt",
			"src/main/pg-sherlock_holmes.txt","src/main/pg-tom_sawyer.txt"}
		m = mr.MakeCoordinator(files, 8)
	} else{
		m = mr.MakeCoordinator(os.Args[1:], 8)
	}

	//files:=[] string{"src/main/pg-being_ernest.txt"}
	//m := mr.MakeMaster(files, len(files))

	for m.Done() == false {
		time.Sleep(time.Second)
		fmt.Fprint(os.Stderr, "master: run...\n")
	}

	time.Sleep(time.Second)
}