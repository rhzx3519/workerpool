package sync_pool

import (
	"fmt"
	"sync"
	"testing"
)

type Flag struct {
	f int32
}


func TestNew(t *testing.T) {
	var cache sync.Pool
	cache.New = func() interface{} {
		return &Flag{
			f: 0,
		}
	}
	x := new(Flag)
	cache.Put(x)
	f1 := cache.Get().(*Flag)
	fmt.Printf("%p\n", x)
	fmt.Printf("%p\n", f1)
}
