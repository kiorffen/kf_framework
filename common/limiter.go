package common

import (
	"sync"
	"sync/atomic"
	"time"
)

// Limiter 限频器接口
type Limiter interface {
	Acquire() bool
}

// LeakyBucketLimiter 漏桶算法限频器 秒级控制速率
type LeakyBucketLimiter struct {
	lastAccessTime time.Time
	capacity       int64
	avail          int64
	mu             sync.Mutex
}

// Acquire 漏桶算法限频器实现
func (l *LeakyBucketLimiter) Acquire() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.avail > 0 {
		l.avail--
		return true
	}
	now := time.Now()
	add := (l.capacity*now.Sub(l.lastAccessTime).Nanoseconds() + 5*1e8) / 1e9
	if add > 0 {
		l.lastAccessTime = now
		l.avail += add
		if l.avail > l.capacity {
			l.avail = l.capacity
		}
		return true
	}
	return false
}

// NewLeakyBucketLimiter 新建一个容量为capacity的漏桶算法限频器
func NewLeakyBucketLimiter(capacity int64) Limiter {
	if capacity <= 0 {
		return nil
	}
	return &LeakyBucketLimiter{capacity: capacity, lastAccessTime: time.Now(), avail: capacity}
}

// TokenBucketLimiter 令牌桶算法限频器 秒级控制速率
type TokenBucketLimiter struct {
	capacity int64
	avail    int64
}

// Acquire 令牌桶算法限频器实现
func (l *TokenBucketLimiter) Acquire() bool {
	if l == nil {
		return true
	}
	if atomic.AddInt64(&l.avail, 1) > l.capacity {
		return false
	}

	return true
}

// NewTokenBucketLimiter 新建一个容量为capacity的令牌桶算法限频器
func NewTokenBucketLimiter(capacity int64) Limiter {
	if capacity <= 0 {
		return nil
	}
	l := &TokenBucketLimiter{capacity: capacity, avail: 0}
	go func() {
		for {
			time.Sleep(time.Second)
			atomic.StoreInt64(&l.avail, 0)
		}
	}()

	return l
}
