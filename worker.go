package workerpool

import (
	"runtime"
	"time"
)

// goWorker 代表一个goroutine, 负责跑task
type goWorker struct {
	// pool who owns this worker.
	pool *Pool

	// task is a job should be done.
	task chan func()

	// recycleTime will be update when putting a worker back into queue.
	recycleTime time.Time
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker) run() {
	w.pool.incRunning()
	go func() { // 起一个goroutine跑任务
		defer func() {
			w.pool.decRunning()
			w.pool.workerCache.Put(w) // 放入cache中
			if p := recover(); p != nil {
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					w.pool.options.Logger.Printf("worker exit with a panic: %v\n", p)
					var buf [4096]byte
					n := runtime.Stack(buf[:], false)
					w.pool.options.Logger.Printf("worker exit with a panic: %s\n", string(buf[:n]))
				}
			}
			// Call Signal() here in case there are goroutines waiting for available workers.
			w.pool.cond.Signal()
		}()

		for f := range w.task {
			if f == nil {
				return
			}
			f() // 运行task
			if ok := w.pool.revertWorker(w); !ok { // 重新将worker放入pool中
				return
			}
		}
	}()
}
