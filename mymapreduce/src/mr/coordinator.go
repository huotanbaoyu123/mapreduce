package mr

import (
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
	 reply.NReduce=c.nReduceTasks
	 reply.NMap=c.nMap
	 reply.Tasktype=Done
	 c.isDone=true
	 return nil


}

func (c *Coordinator)HandleFinishedTask(args *FinishedTaskArgs,reply *FinishedTaskReply)error{
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Tasktype {
	case MapTask:
		c.mapTaskFinished[args.TaskNum]=true
	case TaskReduce:
		c.reduceTaskFinished[args.TaskNum]=true
	default:
		log.Fatal("Bad finished task? %s",args.Tasktype)

	}
	return nil

}