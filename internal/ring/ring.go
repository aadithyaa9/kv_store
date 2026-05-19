package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Ring is a consistent hash ring.
// Internally it is just a sorted slice of uint32 positions.
// The "circle" is conceptual — when a lookup falls off the right end,
// it wraps back to index 0.
type Ring struct {
	mu      sync.RWMutex
	vnodes  int              // how many virtual positions per physical node
	sorted  []uint32         // sorted ring positions (the actual ring)
	nodeMap map[uint32]string // position → physical node address
}

// New creates an empty ring.
// vnodes controls load distribution quality.
// 150 is a good default — used by Cassandra historically.
func New(vnodes int) *Ring {
	return &Ring{
		vnodes:  vnodes,
		sorted:  make([]uint32, 0),
		nodeMap: make(map[uint32]string),
	}
}

// hash produces a uint32 from any string using SHA-256.
// We take the first 4 bytes of the 32-byte SHA-256 digest.
// Why SHA-256 and not MD5/FNV?
//   - MD5: cryptographically broken (irrelevant here but bad habit)
//   - FNV: fast but clusters more — worse distribution on the ring
//   - SHA-256: excellent uniform distribution, acceptable speed for this use
func (r *Ring) hash(key string) uint32 {
	digest := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(digest[:4])
}

// AddNode places a physical node onto the ring by creating `vnodes`
// virtual positions for it. Each vnode is hashed as "addr#0", "addr#1", etc.
//
// After inserting all positions, we re-sort the slice.
// sort.Slice is O(n log n) — acceptable since AddNode is rare (topology changes only).
func (r *Ring) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.vnodes; i++ {
		vkey := fmt.Sprintf("%s#%d", addr, i)
		pos := r.hash(vkey)

		// collision guard: two vnodes landing on the exact same uint32
		// is astronomically rare (1 in 4 billion) but handle it cleanly
		if _, exists := r.nodeMap[pos]; !exists {
			r.nodeMap[pos] = addr
			r.sorted = append(r.sorted, pos)
		}
	}

	sort.Slice(r.sorted, func(i, j int) bool {
		return r.sorted[i] < r.sorted[j]
	})
}

// RemoveNode strips all virtual positions belonging to addr from the ring.
// We rebuild the sorted slice in one pass — O(n).
func (r *Ring) RemoveNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// reuse the same backing array to avoid allocation
	kept := r.sorted[:0]
	for _, pos := range r.sorted {
		if r.nodeMap[pos] == addr {
			delete(r.nodeMap, pos)
		} else {
			kept = append(kept, pos)
		}
	}
	r.sorted = kept
}

// GetNode returns the node responsible for a given key.
// Algorithm:
//   1. Hash the key to a uint32
//   2. Binary search for the first ring position >= that hash
//   3. If we fall off the right end, wrap to index 0 (the ring is circular)
//   4. Return the physical node address at that position
//
// This is O(log n) where n = total vnodes across all nodes.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 {
		return ""
	}

	h := r.hash(key)

	// sort.Search returns the smallest index i in [0, n) such that f(i) is true.
	// Here: smallest index where sorted[i] >= h
	idx := sort.Search(len(r.sorted), func(i int) bool {
		return r.sorted[i] >= h
	})

	// wrap-around: if hash is greater than all positions, use position 0
	if idx == len(r.sorted) {
		idx = 0
	}

	return r.nodeMap[r.sorted[idx]]
}

// Nodes returns the unique physical nodes currently on the ring.
// Used for debugging and the /ring status endpoint.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	for _, addr := range r.nodeMap {
		seen[addr] = true
	}

	nodes := make([]string, 0, len(seen))
	for addr := range seen {
		nodes = append(nodes, addr)
	}
	return nodes
}

// Size returns total virtual node count on the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sorted)
}
