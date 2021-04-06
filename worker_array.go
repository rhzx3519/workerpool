package workerpool

import (
	"errors"
	"time"
)

var (
	errQueueIsFull = errors.New("worker array is full.")

	errQueueIsReleased = errors.New("worker array is empty.")
)

type workerArray interface {
	// 返回当前worker队列中未过期的worker数量
	len() int
	// 队列是否为空
	isEmpty() bool
	// 放入一个新的worker
	insert(worker *goWorker) error
	// 获取一个活跃的worker
	detach() *goWorker
	// 整理队列中已过期的worker，即recycleTime >= time.Now() - duration的goWorker
	retrieveExpiry(duration time.Duration) []*goWorker
	// 重置worker队列
	reset()
}

type arrayType int

const (
	// 栈
	stackType arrayType = 1 << iota
	// 队列
	loopQueueType
)

func newWorkerArray(aType arrayType, size int) workerArray {
	switch aType {
	case stackType:
		return newWorkerStack(size)
	case loopQueueType:
		return newWorkerLoopQueue(size)
	default:
		return newWorkerStack(size)
	}
}

