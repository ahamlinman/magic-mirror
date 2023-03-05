package engine

import "sync"

// NoValue is the standard value type for Engines whose tasks do not produce
// values.
type NoValue = struct{}

// Handler is a type for an Engine's handler function.
type Handler[K comparable, T any] func(K) (T, error)

// Engine is a parallel and deduplicating task runner.
//
// Every unique value provided to GetOrSubmit is mapped to a single Task, which
// will eventually produce a value or an error. The Engine limits the number of
// Tasks that may be in progress at any one time, and does not retry failed
// Tasks.
type Engine[K comparable, T any] struct {
	handle Handler[K, T]

	tasks   map[K]*Task[T]
	tasksMu sync.Mutex

	pending chan K
}

// NewEngine creates an Engine that runs up to `workers` copies of `handle` at
// once to fulfill submitted requests.
func NewEngine[K comparable, T any](workers int, handle Handler[K, T]) *Engine[K, T] {
	e := &Engine[K, T]{
		handle:  handle,
		tasks:   make(map[K]*Task[T]),
		pending: make(chan K),
	}
	for i := 0; i < workers; i++ {
		go e.run()
	}
	return e
}

// NoValueHandler wraps handlers for Engines that produce NoValue, so that the
// handler can be written without a return value type.
func NoValueHandler[K comparable](handle func(K) error) Handler[K, NoValue] {
	return func(key K) (_ NoValue, err error) {
		err = handle(key)
		return
	}
}

// GetOrSubmit returns the unique Task associated with the provided key, either
// by returning an existing Task or scheduling a new one. GetOrSubmit panics if
// called on a closed Engine.
func (e *Engine[K, T]) GetOrSubmit(key K) *Task[T] {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()

	if task, ok := e.tasks[key]; ok {
		return task
	}

	task := &Task[T]{done: make(chan struct{})}
	e.tasks[key] = task
	go func() { e.pending <- key }()
	return task
}

// Close indicates that no more requests will be submitted to the Engine,
// allowing it to eventually shut down. Close panics if called more than once on
// a single Engine.
func (e *Engine[K, T]) Close() {
	close(e.pending)
}

func (e *Engine[K, V]) run() {
	for key := range e.pending {
		e.tasksMu.Lock()
		task := e.tasks[key]
		e.tasksMu.Unlock()

		task.value, task.err = e.handle(key)
		close(task.done)
	}
}

type Task[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func (t *Task[T]) Wait() (T, error) {
	<-t.done
	return t.value, t.err
}
