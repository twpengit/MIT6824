package mapreduce

import (
	"fmt"
	"math/rand"
	"time"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	completedTaskCount := 0
	assignedTaskCount := 0
	workerList := make(map[string]bool)
	workerStatusChannel := make(chan workResult)
	failedTaskList := make([]int, 0)

	// if this is reduce phase, there is no more registration, so we work around by
	// signal registration manualy to let workers start working
	if phase == reducePhase {
		for _, worker := range mr.workers {
			go func(worker string) {
				mr.registerChannel <- worker
			}(worker)
		}
	}

TaskLoop:
	for {
		select {
		case client := <-mr.registerChannel:
			// Add this worker to worker list, and available to work on task
			workerList[client] = true

		case result := <-workerStatusChannel:
			// Worker finish task, avaiable to do next task
			workerList[result.worker] = true

			if result.result {
				// task complete
				completedTaskCount++
				if completedTaskCount == ntasks {
					break TaskLoop
				}
				fmt.Printf("Phase - %s, Task id - %d succeed\n", phase, result.taskNumber)
			} else {
				// Add failed task to the failed task list
				failedTaskList = append(failedTaskList, result.taskNumber)
				fmt.Printf("Phase - %s, Task id - %d failed\n", phase, result.taskNumber)
			}

		default:
			// Assign task to a random picked available worker
			worker := pickWorkerRandomly(workerList)

			if worker == "" {
				continue
			}

			// Check the failed task first
			if len(failedTaskList) > 0 {
				taskNumber := failedTaskList[0]
				fmt.Printf("Assign failed task %d\n", taskNumber)

				args := new(DoTaskArgs)
				args.Phase = phase
				args.JobName = mr.jobName
				args.TaskNumber = taskNumber
				args.File = mr.files[taskNumber]
				args.NumOtherPhase = nios

				// A task assigned to this worker, the worker is not available now
				workerList[worker] = false

				go func(worker string, taskId int) {
					ok := call(worker, "Worker.DoTask", args, new(struct{}))
					if ok {
						workerStatusChannel <- workResult{worker, true, taskId}
					} else {
						workerStatusChannel <- workResult{worker, false, taskId}
					}
				}(worker, taskNumber)

				failedTaskList = failedTaskList[1:len(failedTaskList)]
			} else {
				// Check unassigned task now
				if assignedTaskCount < ntasks {
					// Assign a new task to the worker
					args := new(DoTaskArgs)
					args.Phase = phase
					args.JobName = mr.jobName
					args.TaskNumber = assignedTaskCount
					args.File = mr.files[assignedTaskCount]
					args.NumOtherPhase = nios

					fmt.Printf("Assign new task %d\n", assignedTaskCount)

					// A task assigned to this worker, the worker is not available now
					workerList[worker] = false

					go func(worker string, taskId int) {
						ok := call(worker, "Worker.DoTask", args, new(struct{}))
						if ok {
							workerStatusChannel <- workResult{worker, true, taskId}
						} else {
							workerStatusChannel <- workResult{worker, false, taskId}
						}
					}(worker, assignedTaskCount)

					assignedTaskCount++
				}
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func pickWorkerRandomly(workerList map[string]bool) string {
	validWorkers := make([]string, 0)
	for key, value := range workerList {
		if value {
			validWorkers = append(validWorkers, key)
		}
	}

	if len(validWorkers) > 0 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		idx := r.Intn(len(validWorkers))
		return validWorkers[idx]
	} else {
		return ""
	}
}

type workResult struct {
	worker     string
	result     bool
	taskNumber int
}
