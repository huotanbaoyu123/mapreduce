package mr

//func ihash(key string) int{
//	h:=fnv.New32a()
//	h.Write([]byte(key))
//	return int (h.Sum32()&0x7fffffff)
//}
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"



//
// Map functions return a slice of KeyValue.
//
//type KeyValue struct {
//	Key   string
//	Value string
//}
//type ByKey []KeyValue
//
//func (a ByKey) Len() int           { return len(a) }
//func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//finalizeReduceFile atomiacally renames temporary reduce file to a completed redcue task file
func finalizeReduceFile(tmpFile string ,taskN int) {
	finalFile:=fmt.Sprintf("mr-out-%d",taskN)
	os.Rename(tmpFile,finalFile)
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

        //this will wait unitl we get assingned a task!
        call("Coordinator.HandleGetTask",&ars,&reply)

		switch reply.Tasktype {
		case MapTask:
			performMap(reply.MapFile,reply.TaskNum,reply.NReduceTask,mapf)
		case ReduceTast:
			performReduce(reply.MapFile,reply.NMapTasks,reducef)
		case DoneTask:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s",reply.TaskType)

		}

     	//tell coordinator that we're done
     	finargs:=FinishedTaskArgs{Tasktype: reply.Tasktype,TaskNum: reply.TaskNum}

        finreply:=FinishedTaskReply{}
        call("Coordinator.HandleFinishTask",&finargs,&finreply)

	}

}

func performMap(filename string,taskNum int,nRedcueTasks int,mapf func(string,string)[]KeyValue){
	file,err:=os.Open(filename)
	if err!=nil{
		log.Fatalf("cannot open %v",filename)
	}
	content,err:=ioutil.ReadAll(file)
	if err!=nil{
		log.Fatalf("cannot open %v",filename)
	}
	file.Close()

	//aplly map function to contents of file and collect
	//the set of key-value pairs
	kva:=mapf(filename,string(content))


	//create temporary files and encoders for each file
	tmpFiles:=[]*os.File{}
	tmpFilenames:=[]string{}
	encoders:=[]*json.Encoder{}

	for r:=0 ;r<nRedcueTasks;r++{
		tmpFile,err:=ioutil.TempFile("","")
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
		r:=ihash(kv.key)
		encoders[r].Encode(&kv)
	}
	for _,f:=range tmpFiles{
		f.Close()
	}

	for r:=0;r<nRedcueTasks;r++{
		finalizeIntermediateFile(tmpFilenames[r],taskNum,r)
	}
}

func performReduce(taskNum int,nMapTasks int,reducef func(string,string)[]KeyValue) {
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

	//get temporaray reduce file to write values
	tmpFile,err:=ioutil.TempFile("","")
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
    	for key_end<len(kva)&&kva[key_end].key==kva[key_begin].Key{
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

	//atmoically rename reduce file to final reduce file
	finalizeReduceFile(tmpFilename,taskNum)
}












































