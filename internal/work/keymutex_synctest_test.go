//go:build goexperiment.synctest && go1.25

package work

import (
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

func TestKeyMutexBasicSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const keyCount = 3
		const workerCount = 2 * keyCount

		var (
			km      KeyMutex[int]
			locked  [keyCount]atomic.Int32
			unblock = make(chan struct{})
		)
		for i := range workerCount {
			key := i / 2
			go func() {
				km.Lock(key)
				defer km.Unlock(key)

				locked[key].Add(1)
				defer locked[key].Add(-1)

				<-unblock
			}()
		}

		// Wait for every goroutine to be durably blocked, then check for limit
		// breaches.
		synctest.Wait()
		for i := range locked {
			if count := locked[i].Load(); count > 1 {
				t.Errorf("mutex for %d held %d times", i, count)
			}
		}

		// Let all of the workers finish.
		close(unblock)
	})
}

func TestKeyMutexDetachReattachSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			km       KeyMutex[Empty]
			unblock0 = make(chan struct{})
		)
		q := NewQueue(1, func(qh *QueueHandle, x int) (int, error) {
			if x == 0 {
				km.LockDetached(qh, Empty{})
				<-unblock0
				km.Unlock(Empty{})
			}
			return x, nil
		})

		// Take the lock.
		km.Lock(Empty{})

		// Start the handler for 0, which will have to detach since we're holding
		// the lock.
		go func() { q.Get(0) }()
		synctest.Wait()

		// Ensure that unrelated handlers can, in fact, proceed.
		q.Get(1)

		// Release the lock so handler 0 can obtain it.
		km.Unlock(Empty{})
		synctest.Wait()

		// Start another handler...
		done := make(chan struct{})
		go func() {
			defer close(done)
			q.Get(2)
		}()

		// ...and ensure it really is blocked behind handler 0.
		synctest.Wait()
		select {
		case <-done:
			t.Error("computation of key was not blocked")
		default:
		}

		// Allow all of the handlers to finish.
		close(unblock0)
		keys := []int{0, 1, 2}
		got, _ := q.GetAll(keys...)
		assert.Equal(t, keys, got)
	})
}
