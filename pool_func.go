package workerpool

import (
	"github.com/rhzx3519/workerpool/internal"
	"sync"
	"sync/atomic"
	"time"
)

type PoolWithFunc struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	capacity int32

	// running is the number of the currently running goroutines.
	running int32

	// blockingNum is the number of the goroutines already been blocked on pool.Submit, protected by pool.lock
	blockingNum int

	// workers is a slice that store the available workers.
	workers []*goWorkerWithFunc

	// poolFunc is the function for processing tasks.
	poolFunc func(interface{})

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
func (p *PoolWithFunc) purgePeriodically() {
	heartbeat := time.NewTicker(p.options.ExpiryDuration)
	defer heartbeat.Stop()

	var expiredWorkers []*goWorkerWithFunc
	for range heartbeat.C {
		if p.IsClosed() {
			break
		}
		currentTime := time.Now()

		p.lock.Lock()

		idleWorkers := p.workers

		var i int
		n := len(idleWorkers)
		for ; i < n && currentTime.Sub(idleWorkers[i].recycleTime) > p.options.ExpiryDuration; i++ {}
		expiredWorkers = append(expiredWorkers[:0], idleWorkers[:i]...)
		if i > 0 {
			m := copy(idleWorkers, idleWorkers[i:])
			for i := m; i < n; i++ {
				idleWorkers[i] = nil
			}
			p.workers = idleWorkers[:m]
		}

		p.lock.Unlock()

		for k := range expiredWorkers {
			expiredWorkers[k].task <- nil
			expiredWorkers[k] = nil
		}

		// There might be a situation that all workers have been cleaned up(no any worker is running)
		// while some invokers still get stuck in "p.cond.Wait()",
		// then it ought to wakes all those invokers.
		if p.Running() == 0 {
			p.cond.Broadcast()
		}
	}

}

// NewPoolWithFunc generates an instance of ants pool with a specific function.
func NewPoolWithFunc(size int, pf func(interface{}), actions ...consumer) (*PoolWithFunc, error) {
	if size <= 0 {
		return nil, ErrInvalidPoolSize
	}
	if pf == nil {
		return nil, ErrLackPoolFunc
	}

	opts := loadOptions(actions...)

	if expiry := opts.ExpiryDuration; expiry < 0 {
		return nil, ErrInvalidPoolExpiry
	} else if expiry == 0 {
		opts.ExpiryDuration = DefaultCleanIntervalTime
	}

	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	var p = &PoolWithFunc{
		capacity: int32(size),
		poolFunc: pf,
		lock: internal.NewSpinLock(),
		options: opts,
	}

	p.workerCache.New = func() interface{} {
		return &goWorkerWithFunc{
			pool: p,
			task: make(chan interface{}, workerChanCap),
			recycleTime: time.Now(),
		}
	}
	if p.options.PreAlloc {
		p.workers = make([]*goWorkerWithFunc, 0, size)
	}
	p.cond = sync.NewCond(p.lock)

	// Start a goroutine to clean up expired workers periodically.
	go p.purgePeriodically()

	return p, nil
}

// ----------------------------------------------------------------------------------------------------

func (p *PoolWithFunc) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

func (p *PoolWithFunc) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *PoolWithFunc) Free() int {
	return p.Cap() - p.Running()
}

func (p *PoolWithFunc) Tune(size int) {
	if size <= 0 || size == p.Cap() || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
}

func (p *PoolWithFunc) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Invoke submits a task to pool.
func (p *PoolWithFunc) Invoke(args interface{}) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	var w *goWorkerWithFunc
	if w = p.retrieveWorker(); w == nil {
		return ErrPoolOverload
	}
	w.task <- args
	return nil
}

func (p *PoolWithFunc) Release() {
	atomic.StoreInt32(&p.state, CLOSED)
	p.lock.Lock()

	workers := p.workers
	for _, w := range workers {
		w.task <- nil
	}
	p.workers = nil

	p.lock.Unlock()

	p.cond.Broadcast()
}

func (p *PoolWithFunc) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		go p.purgePeriodically()
	}
}

// ----------------------------------------------------------------------------------------------------

func (p *PoolWithFunc) incRunning() {
	atomic.AddInt32(&p.running, 1)
}

func (p *PoolWithFunc) decRunning() {
	atomic.AddInt32(&p.running, -1)
}

// retrieveWorker returns a available worker to run the tasks.
func (p *PoolWithFunc) retrieveWorker() (w *goWorkerWithFunc) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorkerWithFunc)
		w.run()
	}

	p.lock.Lock()
	idleWorkers := p.workers
	n := len(idleWorkers) - 1
	if n >= 0 {
		w = idleWorkers[n]
		idleWorkers[n] = nil
		p.workers = idleWorkers[:n]
		p.lock.Unlock()
	} else if p.Running() < p.Cap() {
		p.lock.Unlock()
		spawnWorker()
	} else {
		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
	Reentry:
		if p.options.MaxBlockingTasks != 0 && p.blockingNum >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}
		p.blockingNum++
		p.cond.Wait()
		p.blockingNum--
		if p.Running() == 0 {
			p.lock.Unlock()
			if !p.IsClosed() {
				spawnWorker()
			}
			return
		}

		l := len(p.workers) - 1 // p.workers已经更新
		if l < 0 {
			if p.Running() < p.Cap() {
				p.lock.Unlock()
				spawnWorker()
				return
			}
			goto Reentry
		}

		w = p.workers[l]
		p.workers[l] = nil
		p.workers = p.workers[:l]
		p.lock.Unlock()
	}

	return
}

// revertWorker puts a worker back to the pool, recycling the goroutines.
func (p *PoolWithFunc) revertWorker(worker *goWorkerWithFunc) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		return false
	}
	worker.recycleTime = time.Now()
	p.lock.Lock()

	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}

	p.workers = append(p.workers, worker)

	p.cond.Signal()
	p.lock.Unlock()
	return true
}