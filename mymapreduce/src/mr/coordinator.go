package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	//protect coordinator state
	//from concurrent access
	mu sync.Mutex

	//Allow cordinator to wait to assign redcue task until map task have finished.
	//or when all tasks are assinged and are running
	//the coordinator is woken up either when a task has finishded,or if a timeout has expierd
	cond *sync.Cond

	//len(map files)==nMap
	mapFiles []string
	nMapTasks int
	nReduceTasks int

	//Keep track of when task are assingned
	//and which tasks have finished
	mapTaskFinished []bool
	mapTaskIssued []time.Time
	reduceTaskFinished[]bool
	reduceTasksIssued[]time.Time

	//set to rue when all reduce tasks are complete
	isDone bool
}

/*
handle getTask PRC from workder

 */
func (c *Coordinator)HandleGetTask(args *GetTaskArgs,reply *GetTaskReply)error{
	 c.mu.Lock()
	 defer c.mu.Unlock()
	 reply.NReduceTask=c.nReduceTasks
	 reply.NMapTasks=c.nMapTasks

	mapTaskTmp := 0


	 //Issue map tasks until there are no map task left

	 for{
		 mapDone:=true
	    for mapTaskTmp,done:=range c.mapTaskFinished{
	    	if!done {

				iszeor := c.mapTaskIssued[mapTaskTmp].IsZero()
				isOverTime := time.Since(c.mapTaskIssued[mapTaskTmp]).Seconds() > 10
				//assign a task if it's either never been isused,or if it's been too long
				//since it was issued so the worker may crashed
				//note:if task has never been issued,time is initialized to 0 UTC
				if iszeor || isOverTime {
					reply.Tasktype = MapTask
					reply.TaskNum = mapTaskTmp
					reply.MapFile = c.mapFiles[mapTaskTmp]
					c.mapTaskIssued[mapTaskTmp] = time.Now()
					return nil

				} else {
					mapDone = false
				}

			}
		}
		//if all maps are in progress and haven't time out,wait to give another task
        if !mapDone{
        	//todo wait!
        	c.cond.Wait()
		}else{
			//we're dong with all map tasks!
			break
		}


	 }

	 //ALL map tasks are done,issue reduce task now
	 for{
	 	 redDone:=true
	 	 for r,done:=range c.reduceTaskFinished{
	 	 	if !done{
	 	 		//assign a task if it's either never been issued,or if it's been too long
	 	 		//since it was issued so the worker may have crashed
	 	 		//note:if task has never been issued,time is initalized to 0 utc
	 	 		if c.reduceTasksIssued[r].IsZero()||
	 	 			time.Since(c.reduceTasksIssued[r]).Seconds()>10{
	 	 			reply.Tasktype=ReduceTast
	 	 			reply.TaskNum=r
					reply.NMapTasks=mapTaskTmp
	 	 			c.reduceTasksIssued[r]=time.Now()
	 	 			return nil
				}else{
					redDone=false
				}
			}
		 }
		 if !redDone{
		 	//todo wait!
			 c.cond.Wait()
		 }else{
		 	//we're done with all reduce tasks!
		 	break
		 }


	 }

	 //if all map and reduce tasks are done,send the querying worker
	 //a Done TaskType,and set isDone to true
	 reply.Tasktype=DoneTask
	 c.isDone=true




	 return nil


}

func (c *Coordinator)HandleFinishedTask(args *FinishedTaskArgs,reply *FinishedTaskReply)error{

	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Tasktype {
	case MapTask:
		c.mapTaskFinished[args.TaskNum]=true
	case ReduceTast:
		c.reduceTaskFinished[args.TaskNum]=true
	default:
		log.Fatal("Bad finished task? %s",args.Tasktype)

	}
	return nil

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	tt :=masterNetwork()

	l, e := net.Listen(tt, sockname);
	fmt.Print("listen %s %d",tt, sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//main/mrcoordinator.go calls Done() periodcally to find out if the entire job has finished.
func (c *Coordinator)Done() bool{
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}




//create a coordinator
//main/mrcoordinator.go calls this function
//nReduce is the number of reduce tasks to use
func MakeCoordinator(files[] string,nReduce int)*Coordinator{
	//your code here
	c:=Coordinator{}
	c.cond=sync.NewCond(&c.mu)
	c.mapFiles=files
	c.mapTaskFinished=make([]bool,nReduce)
	c.mapTaskIssued=make([]time.Time,nReduce)
	c.nReduceTasks=nReduce
	c.reduceTaskFinished=make([]bool,nReduce)
	c.reduceTasksIssued=make([]time.Time,nReduce)

	//wake up the Gettask handler thread every once in awhile to check if some task hasn't
	//finished,so we can know to reissue it
	go func() {
		for{
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}























































