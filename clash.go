package clash

import (
	"context"
	"errors"
	"sync"
	"time"
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
	items, err := l.batchFn(ctx, keys)
	if err != nil {
		l.cond.L.Lock()
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

	itemByID := make(map[Key]Result)
	for _, item := range items {
		itemByID[item.Key] = item
	}

	l.cond.L.Lock()
	for _, key := range keys {
		item, exists := itemByID[key]
		if exists {
			if item.Error != nil {
				item.status = Failed
			} else {
				item.status = Success
			}
			l.cache[key] = item
		} else {
			l.cache[key] = Result{
				status: Failed,
				Error:  ErrKeyNotFound,
				Value:  nil,
				Key:    key,
			}
		}
	}
	l.cond.Broadcast()
	l.cond.L.Unlock()
}

func (l *Loader) Load(ctx context.Context, key Key) (interface{}, error) {
	l.wg.Add(1)
	defer l.wg.Done()

	l.mu.RLock()
	result, exists := l.cache[key]
	l.mu.RUnlock()

	condition := func() bool {
		l.mu.RLock()
		result := l.cache[key]
		l.mu.RUnlock()

		return result.status == Pending
	}

	if exists {
		if result.status == Success || result.status == Failed {
			return result.Value, result.Error
		}

		l.cond.L.Lock()

		for condition() {
			l.cond.Wait()
		}

		l.mu.RLock()
		result = l.cache[key]
		l.mu.RUnlock()

		l.cond.L.Unlock()
		return result.Value, result.Error
	}

	l.mu.Lock()
	l.cache[key] = Result{status: Pending}
	l.mu.Unlock()

	l.cond.L.Lock()
	for condition() {
		l.cond.Wait()
	}

	l.mu.RLock()
	result = l.cache[key]
	l.mu.RUnlock()

	l.cond.L.Unlock()

	return result.Value, result.Error
}

func (l *Loader) Close() {
	l.once.Do(func() {
		l.wg.Wait()
		close(l.done)
	})
}
