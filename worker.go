package workerpool

import "time"

// goWorker 代表一个goroutine, 负责跑task
type goWorker struct {
	// pool who owns this worker.
	pool *Pool

	// task is a job should be done.
	task chan func()

	// recycleTime will be update when putting a worker back into queue.
	recycleTime time.Time
}
