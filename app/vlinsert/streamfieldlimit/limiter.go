package streamfieldlimit

import (
	"flag"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

var (
	key           = flag.String("insert.streamFieldLimit.key", "", "The Loki stream field name whose value is used for limiting ingested log rows. The limit is disabled if this flag is empty")
	rowsPerWindow = flag.Int("insert.streamFieldLimit.rowsPerWindow", 0, "The maximum number of log rows allowed per -insert.streamFieldLimit.key value during -insert.streamFieldLimit.window. The limit is disabled if this flag is zero")
	window        = flag.Duration("insert.streamFieldLimit.window", 24*time.Hour, "The time window for -insert.streamFieldLimit.rowsPerWindow")
)

var globalLimiter limiter

type limiter struct {
	shards [256]limiterShard
}

type limiterShard struct {
	mu          sync.Mutex
	m           map[limitKey]*entry
	lastCleanup int64
}

type limitKey struct {
	keyHash   uint64
	valueHash uint64
}

type entry struct {
	windowStart int64
	lastSeen    int64
	rows        int
}

// Enabled returns true if stream field value limiting is enabled.
func Enabled() bool {
	return *key != "" && *rowsPerWindow > 0 && *window > 0
}

// Key returns the configured Loki stream field name used for limiting.
func Key() string {
	return *key
}

// AllowN returns the number of log rows with the given stream field value that
// are allowed according to the current stream field value limit configuration.
func AllowN(value string, n int) int {
	if n <= 0 {
		return 0
	}
	if !Enabled() {
		return n
	}
	lk := limitKey{
		keyHash:   xxhash.Sum64String(*key),
		valueHash: xxhash.Sum64String(value),
	}
	return globalLimiter.allowN(lk, n, *rowsPerWindow, (*window).Nanoseconds())
}

// Reset clears the limiter state.
func Reset() {
	globalLimiter.reset()
}

func (l *limiter) allowN(lk limitKey, n, rowsLimit int, windowNsecs int64) int {
	now := time.Now().UnixNano()
	shard := &l.shards[lk.valueHash%uint64(len(l.shards))]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.cleanupLocked(now, windowNsecs)
	if shard.m == nil {
		shard.m = make(map[limitKey]*entry)
	}
	e := shard.m[lk]
	if e == nil {
		e = &entry{
			windowStart: now,
			lastSeen:    now,
		}
		shard.m[lk] = e
	}
	if now-e.windowStart >= windowNsecs {
		e.windowStart = now
		e.rows = 0
	}
	remaining := rowsLimit - e.rows
	if remaining <= 0 {
		e.lastSeen = now
		return 0
	}
	allowed := n
	if allowed > remaining {
		allowed = remaining
	}
	e.rows += allowed
	e.lastSeen = now
	return allowed
}

func (s *limiterShard) cleanupLocked(now, windowNsecs int64) {
	if s.m == nil || windowNsecs <= 0 {
		return
	}
	if s.lastCleanup != 0 && now-s.lastCleanup < windowNsecs {
		return
	}
	s.lastCleanup = now
	for k, e := range s.m {
		if now-e.lastSeen > 2*windowNsecs {
			delete(s.m, k)
		}
	}
}

func (l *limiter) reset() {
	for i := range l.shards {
		shard := &l.shards[i]
		shard.mu.Lock()
		clear(shard.m)
		shard.lastCleanup = 0
		shard.mu.Unlock()
	}
}
