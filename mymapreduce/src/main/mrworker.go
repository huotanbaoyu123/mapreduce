package main
import "../mr"
import "os"
import "fmt"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf:=Map
	//mapf, reducef := loadPlugin(os.Args[1])
	reducef := Reduce

	mr.Worker(mapf, reducef)
}
