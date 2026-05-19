package ring

import (
	"fmt"
	"testing"
)

// TestGetNodeBasic verifies that a key always routes to some node
func TestGetNodeBasic(t *testing.T) {
	r := New(10)
	r.AddNode("localhost:8001")
	r.AddNode("localhost:8002")
	r.AddNode("localhost:8003")

	keys := []string{"user", "product", "cart", "session", "order", "city", "country"}
	for _, key := range keys {
		node := r.GetNode(key)
		if node == "" {
			t.Errorf("key %q routed to empty node", key)
		}
	}
}

// TestConsistency verifies the same key always routes to the same node
// when the ring topology hasn't changed
func TestConsistency(t *testing.T) {
	r := New(150)
	r.AddNode("localhost:8001")
	r.AddNode("localhost:8002")
	r.AddNode("localhost:8003")

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		first := r.GetNode(key)
		second := r.GetNode(key)
		if first != second {
			t.Errorf("key %q returned different nodes: %s vs %s", key, first, second)
		}
	}
}

// TestMinimalDisruption is the core consistent hashing guarantee:
// when a node is added, only ~1/N keys should change ownership.
// With 3 nodes → 4 nodes, ~25% of keys should move. We allow 35% for variance.
func TestMinimalDisruption(t *testing.T) {
	r := New(150)
	r.AddNode("localhost:8001")
	r.AddNode("localhost:8002")
	r.AddNode("localhost:8003")

	total := 10000
	before := make(map[string]string, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = r.GetNode(key)
	}

	// add a 4th node
	r.AddNode("localhost:8004")

	changed := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		if r.GetNode(key) != before[key] {
			changed++
		}
	}

	pct := float64(changed) / float64(total) * 100
	t.Logf("%.1f%% of keys moved when 4th node was added (expected ~25%%)", pct)

	if pct > 35 {
		t.Errorf("too many keys moved: %.1f%% (max allowed 35%%)", pct)
	}
}

// TestRemoveNode verifies that after removal, no key routes to the removed node
func TestRemoveNode(t *testing.T) {
	r := New(150)
	r.AddNode("localhost:8001")
	r.AddNode("localhost:8002")
	r.AddNode("localhost:8003")

	r.RemoveNode("localhost:8002")

	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := r.GetNode(key)
		if node == "localhost:8002" {
			t.Errorf("key %q still routed to removed node", key)
		}
	}
}

// TestDistribution verifies reasonably even key distribution across nodes.
// With vnodes=150, no node should hold more than 50% of keys
// (ideal is 33% with 3 nodes).
func TestDistribution(t *testing.T) {
	r := New(150)
	nodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	counts := make(map[string]int)
	total := 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		counts[r.GetNode(key)]++
	}

	for node, count := range counts {
		pct := float64(count) / float64(total) * 100
		t.Logf("Node %s owns %.1f%% of keys", node, pct)
		if pct > 50 {
			t.Errorf("node %s is overloaded: %.1f%%", node, pct)
		}
	}
}

// TestEmptyRing verifies graceful handling of empty ring
func TestEmptyRing(t *testing.T) {
	r := New(10)
	node := r.GetNode("any-key")
	if node != "" {
		t.Errorf("empty ring should return empty string, got %q", node)
	}
}

// TestWrapAround verifies that keys hashing to the very end of uint32 range
// correctly wrap around to the first node
func TestWrapAround(t *testing.T) {
	r := New(150)
	r.AddNode("localhost:8001")
	r.AddNode("localhost:8002")

	// run many keys — some will hash near uint32 max and trigger wrap-around
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("wrap-test-%d", i)
		node := r.GetNode(key)
		if node == "" {
			t.Errorf("wrap-around key %q routed to empty node", key)
		}
	}
}
