/*
*  SPDX-License-Identifier: AGPL-3.0-only
*  Copyright (c) 2022-2025, daeuniverse Organization <dae@v2raya.org>
 */

package control

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const UdpTaskQueueLength = 2048

const (
	UdpSourceRateLimitPerSec = 2000
	UdpSourceBurst           = 4000
)

type UdpTask = func()

type sourceLimiter struct {
	tokens float64
	last   time.Time
}

// UdpTaskQueue make sure packets with the same key (4 tuples) will be sent in order.
type UdpTaskQueue struct {
	key       string
	p         *UdpTaskPool
	ch        chan UdpTask
	timer     *time.Timer
	agingTime time.Duration
	ctx       context.Context
	closed    chan struct{}
}

func (q *UdpTaskQueue) convoy() {
	for {
		select {
		case <-q.ctx.Done():
			close(q.closed)
			return
		case task := <-q.ch:
			task()
			q.timer.Reset(q.agingTime)
		}
	}
}

type UdpTaskPool struct {
	queueChPool sync.Pool
	// mu protects m
	mu sync.Mutex
	m  map[string]*UdpTaskQueue
	// limiterMu protects limiters
	limiterMu sync.Mutex
	limiters  map[string]*sourceLimiter
	// dropped counts tasks dropped due to full queues.
	dropped uint64
}

func NewUdpTaskPool() *UdpTaskPool {
	p := &UdpTaskPool{
		queueChPool: sync.Pool{New: func() any {
			return make(chan UdpTask, UdpTaskQueueLength)
		}},
		mu:        sync.Mutex{},
		m:         map[string]*UdpTaskQueue{},
		limiterMu: sync.Mutex{},
		limiters:  map[string]*sourceLimiter{},
	}
	return p
}

func (p *UdpTaskPool) allowSource(sourceIP string) bool {
	if sourceIP == "" {
		return true
	}
	now := time.Now()
	burst := float64(UdpSourceBurst)
	rate := float64(UdpSourceRateLimitPerSec)

	p.limiterMu.Lock()
	limiter := p.limiters[sourceIP]
	if limiter == nil {
		limiter = &sourceLimiter{tokens: burst, last: now}
		p.limiters[sourceIP] = limiter
	}
	elapsed := now.Sub(limiter.last).Seconds()
	if elapsed > 0 {
		limiter.tokens += elapsed * rate
		if limiter.tokens > burst {
			limiter.tokens = burst
		}
		limiter.last = now
	}
	if limiter.tokens < 1 {
		p.limiterMu.Unlock()
		return false
	}
	limiter.tokens -= 1
	p.limiterMu.Unlock()
	return true
}

// EmitTask: Make sure packets with the same key (4 tuples) will be sent in order.
func (p *UdpTaskPool) EmitTask(key, sourceIP string, task UdpTask) {
	if !p.allowSource(sourceIP) {
		atomic.AddUint64(&p.dropped, 1)
		return
	}
	p.mu.Lock()
	q, ok := p.m[key]
	if !ok {
		ch := p.queueChPool.Get().(chan UdpTask)
		ctx, cancel := context.WithCancel(context.Background())
		q = &UdpTaskQueue{
			key:       key,
			p:         p,
			ch:        ch,
			timer:     nil,
			agingTime: DefaultNatTimeout,
			ctx:       ctx,
			closed:    make(chan struct{}),
		}
		q.timer = time.AfterFunc(q.agingTime, func() {
			// if timer executed, there should no task in queue.
			// q.closed should not blocking things.
			p.mu.Lock()
			cancel()
			delete(p.m, key)
			p.mu.Unlock()
			<-q.closed
			if len(ch) == 0 { // Otherwise let it be GCed
				p.queueChPool.Put(ch)
			}
		})
		p.m[key] = q
		go q.convoy()
	}
	p.mu.Unlock()
	// If task cannot be executed within 180s(DefaultNatTimeout), GC may be triggered, so skip the task when GC occurs.
	// Use a non-blocking enqueue to avoid UDP backpressure stalling the read loop.
	// When full, drop the oldest task to keep the latest packets.
	for i := 0; i < 2; i++ {
		select {
		case q.ch <- task:
			return
		case <-q.ctx.Done():
			return
		default:
			select {
			case <-q.ch:
				atomic.AddUint64(&p.dropped, 1)
			default:
				atomic.AddUint64(&p.dropped, 1)
				return
			}
		}
	}
	atomic.AddUint64(&p.dropped, 1)
}

func (p *UdpTaskPool) Dropped() uint64 {
	return atomic.LoadUint64(&p.dropped)
}

var (
	DefaultUdpTaskPool = NewUdpTaskPool()
)
