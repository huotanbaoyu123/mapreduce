package mr

import (
	"encoding/json"

	"io/ioutil"

	"os"
	"sort"
)
import "log"
import "fmt"
import "net/rpc"
import "hash/fnv"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
//
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func ihash(key string) int{
	h:=fnv.New32a()
	h.Write([]byte(key))
	return int (h.Sum32()&0x7fffffff)
}
//finalizeReduceFile atomiacally renames temporary reduce file to a completed redcue task file
func finalizeReduceFile(tmpFile string ,taskN int) {
	finalFile:=fmt.Sprintf("mr-out-%d",taskN)
	err:=os.Rename(tmpFile,finalFile)
	if err!=nil{
		log.Fatalf("cannot finalizeReduceFile %s",err)
	}
}

//get name of the intermediate file,given the map and recude task numbers
func getIntermdeiateFile(mapTaskN int,redTaskN int)string{
    return fmt.Sprintf("mr-%d-%d",mapTaskN,redTaskN)
}

//finalizeIntermediateFile atomically renames temporary intermediate files to completed
//intermediate task file

func finalizeIntermediateFile(tmpFile string,mapTaskN int,redTaskN int){
	finalfile:=getIntermdeiateFile(mapTaskN,redTaskN)
	os.Rename(tmpFile,finalfile)
}



func MyWorker(mapf func(string, string) []KeyValue,
		reducef func(string, []string) string){
	for{
        args:=GetTaskArgs{}
        reply:=GetTaskReply{}
		//tell coordinator that we're done

	//	call("Coordinator.HandleFinishedTask",&args,&reply)
        //this will wait unitl we get assingned a task!
        call("Coordinator.HandleGetTask",&args,&reply)

		switch reply.Tasktype {
		case MapTask:
			performMap(reply.MapFile,reply.TaskNum,reply.NReduceTask,mapf)
		case ReduceTast:
			performReduce(reply.TaskNum,reply.NReduceTask,reducef)
		case DoneTask:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s",reply.Tasktype)

		}

		finargs:=FinishedTaskArgs{Tasktype: reply.Tasktype,TaskNum: reply.TaskNum}

		finreply:=FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask",&finargs,&finreply)

	}

}

func performMap(filename string,taskNum int,nRedcueTasks int,mapf func(string,string)[]KeyValue){
	file,err:=os.Open(filename)
	if err!=nil{
		log.Fatalf("cannot open %v",filename)
	}
	content,err:=ioutil.ReadAll(file)
	if err!=nil{
		log.Fatalf("cannot read %v",filename)
	}
	file.Close()

	//aplly map function to contents of file and collect
	//the set of key-value pairs
	kva:=mapf(filename,string(content))


	//create temporary files and encoders for each file
	tmpFiles:=[]*os.File{}
	tmpFilenames:=[]string{}
	encoders:=[]*json.Encoder{}
	var tmpFile *os.File

	for r:=0 ;r<nRedcueTasks;r++{
		if IsLinux!=false{
			tmpFile,err=ioutil.TempFile("","")
		}else{
			tmpFile,err=ioutil.TempFile("mr-tmp","")
		}


		if err !=nil{
			log.Fatalf("can not open tmefile")
		}
		tmpFiles=append(tmpFiles,tmpFile)
		tmpFilename:=tmpFile.Name()
		tmpFilenames=append(tmpFilenames,tmpFilename)
		enc:=json.NewEncoder(tmpFile)
		encoders=append(encoders,enc)
	}

	//write output keys to appropriteat(temporary!) using the provided ihash function
	for _,kv:=range  kva{
		r:=ihash(kv.Key)%nRedcueTasks
		encoders[r].Encode(&kv)
	}
	for _,f:=range tmpFiles{
		f.Close()
	}

	for r:=0;r<nRedcueTasks;r++{
		finalizeIntermediateFile(tmpFilenames[r],taskNum,r)
	}
}

func performReduce(taskNum int,nMapTasks int,reducef func(string,[]string)string) {
	//get all intermediate files corresponding to this reduce task,and collect the
	//cooresponding key-value pairs
	kva:=[]KeyValue{}
	for m:=0;m<nMapTasks;m++{
		ifilename:=getIntermdeiateFile(m,taskNum)
		file,err:=os.Open(ifilename)
		if err!=nil{
			log.Fatalf("cannot open %v",ifilename)
		}
		dec:=json.NewDecoder(file)
		for{
			var kv KeyValue
			if err:=dec.Decode(&kv);err!=nil{
				break
			}
			kva=append(kva,kv)
		}
		file.Close()
	}

	//sort the keys
	sort.Sort(ByKey(kva))
	var tmpFile *os.File
	var err error
	//get temporaray reduce file to write values
	if IsLinux!=false{
		tmpFile,err=ioutil.TempFile("","")
	}else{
		tmpFile,err=ioutil.TempFile("mr-tmp","")
	}
	//tmpFile,err:=ioutil.TempFile("mr-tmp","")
	if err!=nil{
		log.Fatalf("cannot open tmpfile")
	}
	tmpFilename:=tmpFile.Name()

   //apply reduce funciton once to all values of the same key
    key_begin:=0
    for key_begin<len(kva){
    	key_end:=key_begin+1
    	//this loop finds all values with the same keys--they are grouped
    	//together beacause the key are sorted
    	for key_end<len(kva)&&kva[key_end].Key==kva[key_begin].Key{
    		key_end++
		}
		values:=[]string{}
		for k:=key_begin;k<key_end;k++{
			values=append(values,kva[k].Value)
		}
		output:=reducef(kva[key_begin].Key,values)
		//write output to reduce task tmp file
		fmt.Fprintf(tmpFile,"%v %v\n",kva[key_begin].Key,output)

		//go to next key
		key_begin=key_end
	}

	tmpFile.Close()
	//atmoically rename reduce file to final reduce file
	finalizeReduceFile(tmpFilename,taskNum)
}




//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	sockname := masterSock()
	tt :=masterNetwork()
//	c, err := rpc.DialHTTP("unix", "mr-socket")
	c, err := rpc.DialHTTP(tt, sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}







































