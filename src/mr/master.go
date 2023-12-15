package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"../timewheel"
)

type Master struct {
	// Your definitions here.
	taskqueue     chan *Task
	nReduce       int
	TaskTable     []Task
	timer         *timewheel.TimeWheel
	completedNum  int
	phase         State
	intermediates [][]string
	taskTarget    int
}

type State uint32

const (
	Map State = iota
	Reduce
	Exit
)

type Task struct {
	StartTime time.Time
	Input     []string
	NReduce   int
	WorkState State
	TaskNum   int
	Output    []string
	Completed bool
}

var mutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	mutex.Lock()
	mutex.Unlock()
	ret := m.phase == Exit
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskqueue:     make(chan *Task, max(nReduce, len(files))),
		nReduce:       nReduce,
		completedNum:  0,
		phase:         Map,
		timer:         timewheel.NewTimeWheel(11, time.Second),
		TaskTable:     make([]Task, max(len(files), nReduce)),
		intermediates: make([][]string, nReduce),
		taskTarget:    len(files),
	}

	// Your code here.
	for idx, filename := range files {
		task := Task{
			Input:     []string{filename},
			NReduce:   nReduce,
			WorkState: Map,
			TaskNum:   idx,
			Completed: false,
		}
		m.TaskTable[idx] = task
		m.taskqueue <- &task
	}

	m.server()

	return &m
}

func (m *Master) AssignTasks(args *ExampleArgs, reply *Task) error {
	*reply = *<-m.taskqueue
	// 注册超时任务
	m.timer.AddTask(strconv.Itoa(reply.TaskNum), func() {
		// 超时，将任务分配给其它worker
		if m.TaskTable[reply.TaskNum].Completed == false {
			m.taskqueue <- &m.TaskTable[reply.TaskNum]
		}
	}, time.Now().Add(10*time.Second))
	return nil
}

func (m *Master) AllTaskDone() bool {
	if m.taskTarget == m.completedNum {
		return true
	} else {
		return false
	}
}

func (m *Master) TaskCompleted(task *Task, args *ExampleReply) error {
	mutex.Lock()
	index := task.TaskNum
	if m.TaskTable[index].Completed == true {
		mutex.Unlock()
		return nil
	}
	m.TaskTable[index].Completed = true
	m.completedNum++
	mutex.Unlock()
	m.NextStep(task)
	return nil
}

func (m *Master) NextStep(task *Task) {
	mutex.Lock()
	defer mutex.Unlock()
	switch m.phase {
	case Map:
		for idx, intermediate := range task.Output {
			m.intermediates[idx] = append(m.intermediates[idx], intermediate)
		}
		if m.AllTaskDone() {
			m.ToReduce()
		}
	case Reduce:
		if m.AllTaskDone() {
			m.phase = Exit
			m.taskqueue <- &Task{
				WorkState: Exit,
			}
		}
	}
}

func (m *Master) ToReduce() {
	for i := 0; i < len(m.TaskTable); i++ {
		m.TaskTable[i].WorkState = Reduce
		m.TaskTable[i].Completed = false
	}
	m.phase = Reduce
	m.completedNum = 0
	m.taskTarget = m.nReduce

	for i := 0; i < len(m.intermediates); i++ {
		m.TaskTable[i] = Task{
			Input:     m.intermediates[i],
			WorkState: Reduce,
			TaskNum:   i,
			Completed: false,
		}
		m.taskqueue <- &m.TaskTable[i]
	}
}
