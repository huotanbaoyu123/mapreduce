package mr


type Tasktype int
const (
	MapTask   Tasktype = 0
	ReduceTast   Tasktype = 1
	DoneTask   Tasktype = 2

)




/*
getTask RPCs are sent from an idle coordintator to ask for the next task perform
 */

//no arguments to send the coordinator to ask for a task
type GetTaskArgs struct {
}

//Note:PRC fields need to be capitalized in order to be sent
type GetTaskReply struct {
	//what type of task is this?
	Tasktype  Tasktype
	//task number of either map or reduce task
	TaskNum int

	//need for Map(to know which file to write)
	NReduceTask int

	//need for Map(to know which file to read)
	MapFile string

	//need for Map(to know how many intermdeiate map files to read)
	NMapTasks int


}
//Note:RPC fields need to to be capitalized in order to be sent!
/**
finishedTask PRCS are sent from an idle worker to coordinator to indicate that a task has been completed

alternatvie designs can also use 'GetTask' PRCs to send the last task the worker finished,but using a

separeate PRC makes the design cleaner

 */

type FinishedTaskArgs struct{
	Tasktype  Tasktype

	//which task was it?
	TaskNum int
}


type FinishedTaskReply struct{

}