package workerpool

import "time"

type workerStack struct {
	items []*goWorker
	expiry []*goWorker
	size int
}

func (w *workerStack) len() int {
	return len(w.items)
}

func (w *workerStack) isEmpty() bool {
	return w.len() == 0
}

func (w *workerStack) insert(worker *goWorker) error {
	w.items = append(w.items, worker)
	return nil
}

func (w *workerStack) detach() *goWorker {
	if w.isEmpty() {
		return nil
	}
	n := w.len()
	worker := w.items[n-1]
	w.items[n-1] = nil
	w.items = w.items[:n-1]
	return worker
}

func (w *workerStack) retrieveExpiry(duration time.Duration) []*goWorker {
	if w.isEmpty() {
		return nil
	}

	n := w.len()
	expiryTime := time.Now().Add(-duration)
	index := w.binarySearch(0, n-1, expiryTime)
	w.expiry = w.expiry[:0]
	if index != -1 {
		w.expiry = append(w.expiry, w.items[:index+1]...)
		m := copy(w.items, w.items[index+1:])
		for i := m; i < n; i++ {
			w.items[i] = nil
		}
		w.items = w.items[:m]
	}

	return w.expiry
}

func (w *workerStack) reset() {
	for i := 0; i < w.len(); i++ {
		w.items[i].task <- nil
		w.items[i] = nil
	}
	w.items = w.items[:0]
	w.expiry = w.expiry[:0]
}

// 返回过期的元素的索引
func (w *workerStack) binarySearch(l, r int, expiryTime time.Time) int {
	var mid int
	for l <= r {
		mid = l + (r-l)/2
		// expiryTime < recycleTime
		if expiryTime.Before(w.items[mid].recycleTime) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return r
}

func newWorkerStack(size int) *workerStack {
	return &workerStack{
		items: make([]*goWorker, 0, size),
		expiry: make([]*goWorker, 0, size),
		size: size,
	}
}
