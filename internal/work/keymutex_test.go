package work

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyMutexBasic(t *testing.T) {
	const (
		nKeys    = 3
		nWorkers = nKeys * 2
	)

	var (
		km      KeyMutex[int]
		ready   = make(chan struct{})
		locked  [nKeys]atomic.Int32
		unblock = make(chan struct{})
		done    = make(chan struct{}, nWorkers)
	)
	for i := 0; i < nWorkers; i++ {
		key := i / 2
		go func() {
			defer func() { done <- struct{}{} }()
			<-ready

			if err := km.LockDetached(context.Background(), key); err != nil {
				panic(err)
			}
			defer km.Unlock(key)

			locked[key].Add(1)
			defer locked[key].Add(-1)
			<-unblock
		}()
	}

	for i := 0; i < nWorkers; i++ {
		ready <- struct{}{}
	}
	forceRuntimeProgress(nWorkers)
	for i := range locked {
		if count := locked[i].Load(); count > 1 {
			t.Errorf("mutex for %d held %d times", i, count)
		}
	}

	close(unblock)
	timeout := time.After(2 * time.Second)
	for i := 0; i < nWorkers; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatalf("timed out waiting for test goroutines to finish")
		}
	}
}

func TestKeyMutexDetach(t *testing.T) {
	const submitCount = 5

	var (
		km      KeyMutex[NoValue]
		started = make(chan struct{}, submitCount)
		locked  atomic.Bool
	)
	q := NewQueue(1, func(ctx context.Context, x int) (int, error) {
		started <- struct{}{}

		if err := km.LockDetached(ctx, NoValue{}); err != nil {
			return x, err
		}
		defer km.Unlock(NoValue{})

		if !locked.CompareAndSwap(false, true) {
			t.Errorf("more than one queue handler holding the lock")
		}
		defer locked.Store(false)
		return x, nil
	})

	if err := km.LockDetached(context.Background(), NoValue{}); err != nil {
		t.Fatal(err)
	}

	keys := makeIntKeys(submitCount)
	tasks := q.GetOrSubmitAll(keys...)
	timeout := time.After(2 * time.Second)
	for i := range tasks {
		select {
		case <-started:
		case <-timeout:
			t.Fatalf("timed out waiting for tasks to detach: %d of %d running", i, len(tasks))
		}
	}

	km.Unlock(NoValue{})
	assertTaskSucceedsWithin[[]int](t, 2*time.Second, tasks, keys)
}
