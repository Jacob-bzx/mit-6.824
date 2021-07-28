package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(ioutil.Discard)

	workerId, nMap, nReduce := RegisterWorker()
	//f, err := os.OpenFile("log_worker_"+strconv.Itoa(workerId), os.O_RDWR|os.O_CREATE, 0666)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.Printf("Worker %d started, NMap:%d, NReduce:%d.\n", workerId, nMap, nReduce)

	for {
		if CheckCoordinatorDone() {
			log.Printf("MapReduce Done. Close Worker %d.\n", workerId)
			return
		}
		hasTask, taskType, taskId, filename, finished := FetchTask(workerId)
		//log.Printf("HasTask:%t, taskType:%s, taskId:%s, filename:%s", hasTask, taskType, taskId, filename)
		if finished {
			log.Printf("MapReduce task finished. Worker:%d exit!\n", workerId)
			break
		}
		if !hasTask {
			log.Printf("Worker %d receive no task from Coordinator!", workerId)
			time.Sleep(time.Second * 5)
			continue
		}

		switch taskType {
		case MapTaskPrefix:
			log.Printf("Worker %d starts task %s-%s", workerId, taskType, taskId)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v\n", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v\n", filename)
			}
			file.Close()
			intermediateKV := mapf(filename, string(content))
			mapId, err := strconv.Atoi(taskId)
			if err != nil {
				return
			}
			intermediateFiles, encoders := CreateIntermediateFiles(mapId, nReduce)
			defer CloseFiles(intermediateFiles)

			for _, keyValuePair := range intermediateKV {
				R := ihash(keyValuePair.Key) % nReduce
				err = encoders[R].Encode(&keyValuePair)
				if err != nil {
					log.Printf("encoding %#v failed, err:%v\n", keyValuePair, err)
				}
			}

			err = Rename(intermediateFiles)
			if err != nil {
				log.Printf("Rename map files err:%v", err)
			}
			log.Printf("Worker %d finishes task %s-%s", workerId, taskType, taskId)

		case ReduceTaskPrefix:
			log.Printf("Worker %d starts task %s-%s", workerId, taskType, taskId)

			var intermediateKV []KeyValue
			for i := 0; i < nMap; i++ {
				filename := fmt.Sprintf("mr-%d-%s", i, taskId)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v\n, err:%v", filename, err)
				}
				defer file.Close()

				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					intermediateKV = append(intermediateKV, kv)
				}
			}

			sort.Sort(ByKey(intermediateKV))

			oname := fmt.Sprintf("mr-out-%s", taskId)
			ofile, err := ioutil.TempFile(TmpDirPath, fmt.Sprintf("%s.tmp*", filename))
			defer ofile.Close()

			if err != nil {
				log.Fatal(err)
			}
			i := 0
			for i < len(intermediateKV) {
				j := i + 1
				for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediateKV[k].Value)
				}
				output := reducef(intermediateKV[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediateKV[i].Key, output)

				i = j
			}

			err = os.Rename(ofile.Name(), oname)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Worker %d finishes task %s-%s\n", workerId, taskType, taskId)

		default:
			log.Fatalf("Unexpected taskType:%s\n", taskType)
		}
	}

}

func CreateIntermediateFiles(mapId int, nReduce int) ([]*os.File, []*json.Encoder) {
	var files []*os.File
	var encoders []*json.Encoder
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, i)
		file, err := ioutil.TempFile(TmpDirPath, fmt.Sprintf("%s.tmp*", filename))
		if err != nil {
			log.Printf("Create file:%s failed, err:%v\n", filename, err)
			continue
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}
	return files, encoders
}

func CloseFiles(files []*os.File) {
	for i := range files {
		files[i].Close()
	}
}

func Rename(files []*os.File) error {
	for _, file := range files {
		splits := strings.Split(file.Name(), ".")
		err := os.Rename(file.Name(), splits[0])
		if err != nil {
			return err
		}
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func RegisterWorker() (int, int, int) {
	req := RegisterWorkerRequest{}
	rsp := RegisterWorkerResponse{}
	call("Coordinator.RegisterWorker", &req, &rsp)
	return rsp.Id, rsp.NMap, rsp.NReduce
}

func CheckCoordinatorDone() bool {
	req := CheckCoordinatorDoneRequest{}
	rsp := CheckCoordinatorDoneResponse{}
	call("Coordinator.CheckCoordinatorDone", &req, &rsp)
	return rsp.Finished
}

func FetchTask(workId int) (bool, string, string, string, bool) {
	req := FetchTaskRequest{WorkId: workId}
	rsp := FetchTaskResponse{}
	call("Coordinator.FetchTask", &req, &rsp)
	splits := strings.Split(rsp.TaskTypeAndId, "-")
	if len(splits) != 2 {
		return rsp.HasTask, "", "", rsp.Filename, rsp.CoordinatorFinished
	}
	return rsp.HasTask, splits[0], splits[1], rsp.Filename, rsp.CoordinatorFinished
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", CoordinatorIPAndPort)
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
