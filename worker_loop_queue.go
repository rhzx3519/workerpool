package workerpool

import "time"

type loopQueue struct {
	items []*goWorker
	expiry []*goWorker
	head int
	tail int
	size int
	isFull bool
}

func (this *loopQueue) len() int {
	if this.size == 0 {
		return 0
	}

	if this.head == this.tail {
		if this.isFull {
			return this.size
		}
		return 0
	}

	if this.tail > this.head {
		return this.tail - this.head
	}
	return this.size - this.head + this.tail
}

func (this *loopQueue) isEmpty() bool {
	return this.head == this.tail && !this.isFull
}

func (this *loopQueue) insert(worker *goWorker) error {
	if this.size == 0 {
		return errQueueIsReleased
	}
	if this.isFull {
		return errQueueIsFull
	}

	this.items[this.tail] = worker
	this.tail++

	if this.tail == this.size {
		this.tail = 0
	}

	if this.tail == this.head {
		this.isFull = true
	}

	return nil
}

func (this *loopQueue) detach() *goWorker {
	if this.isEmpty() {
		return nil
	}
	w := this.items[this.head]
	this.items[this.head] = nil
	this.head++

	if this.head == this.size {
		this.head = 0
	}

	this.isFull = false

	return w
}

func (this *loopQueue) retrieveExpiry(duration time.Duration) []*goWorker {
	if this.isEmpty() {
		return nil
	}

	this.expiry = this.expiry[:0]
	expiryTime := time.Now().Add(-duration)
	for !this.isEmpty()	{
		if expiryTime.Before(this.items[this.head].recycleTime) {
			break
		}
		this.expiry = append(this.expiry, this.items[this.head])
		this.items[this.head] = nil
		this.head++
		if this.head == this.size {
			this.head = 0
		}
		this.isFull = false
	}
	return this.expiry
}

func (this *loopQueue) reset() {
	if this.isEmpty() {
		return
	}
Releaseing:
	if w := this.detach(); w != nil {
		w.task <- nil
		goto Releaseing
	}
	this.items = this.items[:0]
	this.expiry = this.expiry[:0]
	this.head = 0
	this.tail = 0
	this.size = 0
}

func newWorkerLoopQueue(size int) *loopQueue {
	return &loopQueue{
		items: make([]*goWorker, size),
		size: size,
	}
}
