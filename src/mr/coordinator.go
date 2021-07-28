package mr

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mutex                 sync.Mutex
	RWLock                sync.RWMutex
	currentNumberOfWorker int
	running               bool

	files             []string
	runningMapTask    int
	runningReduceTask int
	totalMapTask      int
	totalReduceTask   int
	MapReadyTasks     chan *TaskInfo
	ReduceReadyTasks  chan *TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Id = c.currentNumberOfWorker
	reply.NMap = c.totalMapTask
	reply.NReduce = c.totalReduceTask
	c.currentNumberOfWorker++
	log.Printf("Worker %d registers successfully\n", reply.Id)
	return nil
}

func (c *Coordinator) CheckCoordinatorDone(args *CheckCoordinatorDoneRequest, reply *CheckCoordinatorDoneResponse) error {
	c.RWLock.RLock()
	reply.Finished = !c.running
	c.RWLock.RUnlock()

	return nil
}

func (c *Coordinator) FetchTask(args *FetchTaskRequest, reply *FetchTaskResponse) error {
	log.Printf("Worker %d fetch task request\n", args.WorkId)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.MapReadyTasks) == 0 && c.runningMapTask > 0 {
		log.Println("Waiting all maps finish!")
		reply.HasTask = false
		reply.CoordinatorFinished = false
		log.Printf("Coordinator assigns no task to Worker %d\n", args.WorkId)
	} else if len(c.MapReadyTasks) > 0 {
		taskInfo := <-c.MapReadyTasks
		taskInfo.status = StatusRunning
		taskInfo.workerId = args.WorkId
		reply.HasTask = true
		reply.CoordinatorFinished = false
		reply.TaskTypeAndId = fmt.Sprintf("%s-%d", MapTaskPrefix, taskInfo.index)
		reply.Filename = c.files[taskInfo.index]

		c.runningMapTask++

		log.Printf("Coordinator assigns a Map task(%d) to Worker %d\n", taskInfo.index, args.WorkId)
		go func(task *TaskInfo) {
			ctx, _ := context.WithTimeout(context.Background(), MaxProcessTime)
			var filenames []string
			for i := 0; i < c.totalReduceTask; i++ {
				filenames = append(filenames, fmt.Sprintf("mr-%d-%d", task.index, i))
			}
		loop:
			for {
				select {
				case <-ctx.Done():
					log.Printf("Worker:%d Map task:%d timeout!\n", args.WorkId, taskInfo.index)
					task.workerId = -1
					task.status = StatusReady
					c.MapReadyTasks <- task
					break loop
				default:
					for _, filename := range filenames {
						if _, err := os.Stat(filename); os.IsNotExist(err) {
							//log.Printf("File %s not exists! Continue waiting!\n", filename)
							time.Sleep(time.Second)
							continue loop
						} else if err != nil {
							log.Fatalf("Unknown Error! err:%v", err)
						}
					}
					taskInfo.status = StatusFinished
					break loop
				}
			}
			c.mutex.Lock()
			c.runningMapTask--
			c.mutex.Unlock()
		}(taskInfo)
	} else if len(c.ReduceReadyTasks) > 0 {
		taskInfo := <-c.ReduceReadyTasks
		taskInfo.status = StatusRunning
		taskInfo.workerId = args.WorkId
		reply.HasTask = true
		reply.CoordinatorFinished = false
		reply.TaskTypeAndId = fmt.Sprintf("%s-%d", ReduceTaskPrefix, taskInfo.index)
		log.Printf("Coordinator assigns a Reduce(%d) task to Worker %d\n", taskInfo.index, args.WorkId)

		c.runningReduceTask++

		go func(task *TaskInfo) {
			ctx, _ := context.WithTimeout(context.Background(), MaxProcessTime)
			filename := fmt.Sprintf("mr-out-%d", taskInfo.index)

		loop:
			for {
				select {
				case <-ctx.Done():
					log.Printf("Worker:%d Reduce task:%d timeout!\n", args.WorkId, taskInfo.index)
					task.workerId = -1
					task.status = StatusReady
					c.ReduceReadyTasks <- task
					break loop
				default:
					if _, err := os.Stat(filename); os.IsNotExist(err) {
						log.Printf("File %s not exists! Continue waiting!\n", filename)
						time.Sleep(time.Second)
						continue loop
					} else if err != nil {
						log.Fatalf("Unknown Error! err:%v", err)
					}

					taskInfo.status = StatusFinished
					break loop
				}
			}
			c.mutex.Lock()
			c.runningReduceTask--
			c.mutex.Unlock()
		}(taskInfo)

	} else if len(c.ReduceReadyTasks) == 0 && c.runningReduceTask > 0 {
		log.Println("Waiting all reduces finish!")
	} else {
		reply.HasTask = false
		reply.CoordinatorFinished = true
		log.Printf("Coordinator assigns no task to Worker %d\n", args.WorkId)
		return nil

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.RWLock.RLock()
	defer c.RWLock.RUnlock()
	return !c.running
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("Files:%+v\n", files)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(ioutil.Discard)
	//f, err := os.OpenFile("log_coordinator", os.O_RDWR | os.O_CREATE, 0666)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.SetOutput(io.MultiWriter(os.Stdout, f))
	c := Coordinator{
		currentNumberOfWorker: 0,
		running:               true,
		files:                 files,
		runningMapTask:        0,
		runningReduceTask:     0,
		totalMapTask:          len(files),
		totalReduceTask:       nReduce,
	}
	c.MapReadyTasks = make(chan *TaskInfo, c.totalMapTask)
	c.ReduceReadyTasks = make(chan *TaskInfo, c.totalReduceTask)

	for i := 0; i < c.totalMapTask; i++ {
		c.MapReadyTasks <- &TaskInfo{
			index:    i,
			taskType: MapTaskPrefix,
			status:   StatusReady,
			workerId: -1,
		}
	}

	for i := 0; i < c.totalReduceTask; i++ {
		c.ReduceReadyTasks <- &TaskInfo{
			index:    i,
			taskType: ReduceTaskPrefix,
			status:   StatusReady,
			workerId: -1,
		}
	}

	c.server()

	go func() {

		for {
			c.mutex.Lock()
			sum := c.runningReduceTask + len(c.ReduceReadyTasks)
			c.mutex.Unlock()

			if sum == 0 {
				break
			}
			time.Sleep(time.Second * 15)
		}
		c.RWLock.Lock()
		c.running = false
		c.RWLock.Unlock()
	}()
	return &c
}

type TaskInfo struct {
	index    int
	workerId int
	taskType string
	status   int
}
