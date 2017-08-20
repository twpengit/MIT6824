package mapreduce

import "fmt"

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
	workerStatusChannel := make(chan workResult)

	// if this is reduce phase, there is no more registration, so we work around by
	// signal fail result for each worker to let workers start to work
	//	if phase == reducePhase {
	//		for idx, worker := range mr.workers {
	//			if idx < ntasks {
	//				go func() {
	//					fmt.Printf("reduce case %s,%d\n", worker, idx)
	//					workerStatusChannel <- workResult{worker, false, idx}
	//				}()
	//			}
	//		}
	//	}

TaskLoop:
	for {
		select {
		case client := <-mr.registerChannel:
			if assignedTaskCount < ntasks {
				fmt.Printf("New client registered, %s\n", client)
				// the worker has already been added to mr.workers
				// assign task to this worker
				args := new(DoTaskArgs)
				args.Phase = phase
				args.JobName = mr.jobName
				args.TaskNumber = assignedTaskCount
				args.File = mr.files[assignedTaskCount]
				args.NumOtherPhase = nios

				//case mapPhase:
				//	doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
				//case reducePhase:
				//	doReduce(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)

				go func() {
					ok := call(client, "Worker.DoTask", args, new(struct{}))
					if ok {
						workerStatusChannel <- workResult{client, true, assignedTaskCount}
					} else {
						workerStatusChannel <- workResult{client, false, assignedTaskCount}
					}
				}()

				assignedTaskCount++
			}

		case result := <-workerStatusChannel:
			// Worker finish task, avaiable to do next task
			if result.result {
				// task complete
				completedTaskCount++
				if completedTaskCount == ntasks {
					break TaskLoop
				}

				if assignedTaskCount < ntasks {
					// Assign a new task to the worker
					args := new(DoTaskArgs)
					args.Phase = phase
					args.JobName = mr.jobName
					args.TaskNumber = assignedTaskCount
					args.File = mr.files[assignedTaskCount]
					args.NumOtherPhase = nios

					go func() {
						ok := call(result.worker, "Worker.DoTask", args, new(struct{}))
						if ok {
							workerStatusChannel <- workResult{result.worker, true, assignedTaskCount}
						} else {
							workerStatusChannel <- workResult{result.worker, false, assignedTaskCount}
						}
					}()

					assignedTaskCount++
				}
			} else {
				// Assign the failed task to the worker again
				args := new(DoTaskArgs)
				args.Phase = phase
				args.JobName = mr.jobName
				args.TaskNumber = result.fileIndex
				args.File = mr.files[result.fileIndex]
				args.NumOtherPhase = nios

				fmt.Printf("reduce case %s,%d\n", args.Phase, args.TaskNumber)

				//case mapPhase:
				//	doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
				//case reducePhase:
				//	doReduce(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)

				go func() {
					ok := call(result.worker, "Worker.DoTask", args, new(struct{}))
					if ok {
						workerStatusChannel <- workResult{result.worker, true, result.fileIndex}
					} else {
						workerStatusChannel <- workResult{result.worker, false, result.fileIndex}
					}
				}()
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

type workResult struct {
	worker    string
	result    bool
	fileIndex int
}
