package workerpool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pool accepts the tasks from client, it limits the total of goroutines to a given number by recycling goroutines.
type Pool struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	capacity int32

	// running is the number of the currently running goroutines.
	running int32

	// blockingNum is the number of the goroutines already been blocked on pool.Submit, protected by pool.lock
	blockingNum int

	// workers is a slice that store the available workers.
	workers workerArray

	// state is used to notice the pool to closed itself.
	state int32

	// lock for synchronous operation.
	lock sync.Locker

	// cond for waiting to get a idle worker.
	cond *sync.Cond

	// workerCache speeds up the obtainment of the an usable worker in function:retrieveWorker.
	workerCache sync.Pool

	// property setting of the pool
	options *Options
}

// purgePeriodically clears expired workers periodically which runs in an individual goroutine, as a scavenger.
func (p *Pool) purgePeriodically() {
	heartbeat := time.NewTicker(p.options.ExpiryDuration)
	defer heartbeat.Stop()

	for range heartbeat.C {
		if p.IsClosed() {
			break
		}

		p.lock.Lock()
		expiredWorkers := p.workers.retrieveExpiry(p.options.ExpiryDuration)
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		for i := range expiredWorkers {
			expiredWorkers[i].task <- nil
			expiredWorkers[i] = nil
		}

		// There might be a situation that all workers have been cleaned up(no any worker is running)
		// while some invokers still get stuck in "p.cond.Wait()",
		// then it ought to wakes all those invokers.
		if p.Running() == 0 {
			p.cond.Broadcast()
		}
	}
}

// NewPool generates an instance of  workerpool.
func NewPool(size int, applys ...apply) (*Pool, error) {
	opts := loadOptions(applys...)

	if size <= 0 {
		size = -1
	}

	if expiry := opts.ExpiryDuration; expiry < 0 {
		return nil, ErrInvalidPoolExpiry
	} else if expiry == 0 {
		expiry = DefaultCleanIntervalTime
	}

	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	var p = &Pool{
		capacity: int32(size),
		lock:     internal.NewSpinLock(),
	} //
	return p, nil
}


// ---------------------------------------------------------------------------

// Running returns the number of the currently running goroutines.
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *Pool) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}


// ---------------------------------------------------------------------------


