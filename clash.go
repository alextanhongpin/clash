package clash

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Key represents the key to load.
type Key string

func (k Key) String() string {
	return string(k)
}

// ErrKeyNotFound ...
var ErrKeyNotFound = errors.New("key not found")

type Request struct {
	ch  chan Result
	key Key
}

type Status int

const (
	None Status = iota
	Pending
	Success
	Failed
)

type Result struct {
	Key    Key
	Value  interface{}
	Error  error
	status Status
}

type Loader struct {
	mu            sync.RWMutex
	wg            sync.WaitGroup
	once          sync.Once
	batchFn       BatchFn
	cond          *sync.Cond
	cache         map[Key]Result
	batchDuration time.Duration
	done          chan bool
}

type BatchFn func(ctx context.Context, keys []Key) ([]Result, error)

func NewLoader(batchFn BatchFn) *Loader {
	l := &Loader{
		batchFn:       batchFn,
		batchDuration: 16 * time.Millisecond,
		cache:         make(map[Key]Result),
		cond:          sync.NewCond(&sync.Mutex{}),
		done:          make(chan bool),
	}
	go l.pool()
	return l
}

func (l *Loader) pool() {
	for {
		select {
		case <-time.After(l.batchDuration):
			l.batch()
		case <-l.done:
			return
		}
	}
}

func (l *Loader) batch() {
	l.mu.Lock()
	defer l.mu.Unlock()

	var keys []Key
	for key := range l.cache {
		result := l.cache[key]
		if result.status == Pending {
			keys = append(keys, key)
		}
	}

	ctx := context.Background()
	results, err := l.batchFn(ctx, keys)
	if err != nil {
		l.cond.L.Lock()
		// On failure, all the result must be set to err.
		// Otherwise it will block the whole operation since the sync.Cond
		// condition will never be fulfilled if the key is not found.
		for _, key := range keys {
			l.cache[key] = Result{
				Key:    key,
				Value:  nil,
				Error:  err,
				status: Failed,
			}
		}
		l.cond.Broadcast()
		l.cond.L.Unlock()
		return
	}

	resultByKey := make(map[Key]Result)
	for _, result := range results {
		resultByKey[result.Key] = result
	}

	l.cond.L.Lock()
	// Ensure that all requested keys have results.
	// Otherwise sync.Cond will block indefinitely.
	for _, key := range keys {
		result, exists := resultByKey[key]
		if exists {
			if result.Error != nil {
				result.status = Failed
			} else {
				result.status = Success
			}
			l.cache[key] = result
		} else {
			l.cache[key] = Result{
				status: Failed,
				Error:  ErrKeyNotFound,
				Value:  nil,
				Key:    key,
			}
		}
	}
	// Broadcast will notify all reader that the key have been populated.
	// Those reader might not necessarily match the key though, but it is fine.
	l.cond.Broadcast()
	l.cond.L.Unlock()
}

func (l *Loader) Set(ctx context.Context, result Result) {
	if result.Error != nil {
		result.status = Failed
	} else {
		result.status = Success
	}
	l.mu.Lock()
	l.cache[result.Key] = result
	l.mu.Unlock()
}

func (l *Loader) LoadMany(ctx context.Context, keys []Key) ([]Result, error) {
	result := make([]Result, len(keys))

	g := new(errgroup.Group)
	for i, key := range keys {
		i, key := i, key
		g.Go(func() error {
			result[i] = l.load(ctx, key)
			return result[i].Error
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (l *Loader) Load(ctx context.Context, key Key) (interface{}, error) {
	result := l.load(ctx, key)
	return result.Value, result.Error
}

func (l *Loader) load(ctx context.Context, key Key) Result {
	l.wg.Add(1)
	defer l.wg.Done()

	l.mu.RLock()
	result, exists := l.cache[key]
	l.mu.RUnlock()

	if exists {
		if result.status == Success || result.status == Failed {
			return result
		}
	} else {
		l.mu.Lock()
		l.cache[key] = Result{status: Pending}
		l.mu.Unlock()
	}

	l.cond.L.Lock()
	for l.isPending(key) {
		l.cond.Wait()
	}

	l.mu.RLock()
	result = l.cache[key]
	l.mu.RUnlock()

	l.cond.L.Unlock()

	return result
}

func (l *Loader) Close() {
	l.once.Do(func() {
		l.wg.Wait()
		close(l.done)
	})
}

func (l *Loader) isPending(key Key) bool {
	l.mu.RLock()
	result := l.cache[key]
	l.mu.RUnlock()

	return result.status == Pending
}
