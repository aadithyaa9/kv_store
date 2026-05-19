package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/aadithyaa9/kv_store/internal/ring"
	"github.com/aadithyaa9/kv_store/internal/store"
)

// Node is both a data store AND a coordinator.
//
// In Level 1, every node stored every key. There was no routing.
// In Level 2, each node owns a slice of the keyspace (determined by the ring).
// When a node receives a request for a key it doesn't own,
// it FORWARDS the request to the correct owner. The client never needs to know.
//
// This pattern is called "coordinator node" — any node can handle any request,
// but only the owner stores/reads the data.
type Node struct {
	Addr string     // this node's own address e.g. "localhost:8001"
	Ring *ring.Ring // full ring — every node has a complete copy
	KV   *store.KV  // local storage — only keys this node owns

	// shared HTTP client — do NOT create a new one per request
	// creating http.Client per-request leaks connections and ignores connection pooling
	client *http.Client
}

// PutRequest is used for both /put (client→node) and /forward/put (node→node)
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// GetResponse is the response shape for /get
type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NodeRequest is used for /nodes/add and /nodes/remove
type NodeRequest struct {
	Addr string `json:"addr"`
}

func New(addr string, peers []string, vnodes int) *Node {
	r := ring.New(vnodes)

	// add self first
	r.AddNode(addr)

	// add all known peers
	for _, peer := range peers {
		r.AddNode(peer)
	}

	return &Node{
		Addr: addr,
		Ring: r,
		KV:   store.New(),
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// Put is called when a client writes a key-value pair.
//
// Decision tree:
//   - Hash the key → find owner on the ring
//   - If owner == self → store locally (base case)
//   - If owner != self → forward to owner
//
// The owner stores it locally when it receives the forwarded request.
// No infinite forwarding: when the owner receives the request, it IS the owner,
// so it stores locally and returns.
func (n *Node) Put(key, value string) error {
	owner := n.Ring.GetNode(key)

	if owner == n.Addr {
		n.KV.Put(key, value)
		log.Printf("[PUT] key=%q stored locally", key)
		return nil
	}

	log.Printf("[PUT] key=%q forwarding to owner %s", key, owner)
	return n.forwardPut(owner, key, value)
}

// Get is called when a client reads a key.
// Same routing logic as Put — owner handles it.
func (n *Node) Get(key string) (string, bool, error) {
	owner := n.Ring.GetNode(key)

	if owner == n.Addr {
		val, ok := n.KV.Get(key)
		return val, ok, nil
	}

	log.Printf("[GET] key=%q forwarding to owner %s", key, owner)
	return n.forwardGet(owner, key)
}

// LocalPut writes directly to local KV without ring lookup.
// Used ONLY when:
//   1. This node IS the owner (called from Put above)
//   2. A forwarded request arrives (the sender already did the ring lookup)
//
// Never call this from external handlers for a new client request.
func (n *Node) LocalPut(key, value string) {
	n.KV.Put(key, value)
}

// AddNode adds a new physical node to this node's ring view.
// In a production system, this would be broadcast to all nodes via gossip.
// Here, you call /nodes/add on each node manually.
func (n *Node) AddNode(addr string) {
	n.Ring.AddNode(addr)
	log.Printf("[RING] added node %s | ring size now %d vnodes", addr, n.Ring.Size())
}

// RemoveNode removes a physical node from this node's ring view.
// Critical: call this BEFORE the node goes down in graceful shutdown.
// After removal, keys that were owned by the removed node are now owned
// by the next node clockwise — they will route there automatically.
// Physical key migration (copying actual data) is a separate concern
// handled by MigrateKeys.
func (n *Node) RemoveNode(addr string) {
	n.Ring.RemoveNode(addr)
	log.Printf("[RING] removed node %s | ring size now %d vnodes", addr, n.Ring.Size())
}

// MigrateKeys is called when THIS node is shutting down gracefully.
// It scans all locally stored keys and pushes any key whose ring-assigned
// owner (after self-removal) is a different node.
//
// Call sequence for graceful shutdown:
//   1. n.RemoveNode(n.Addr)        — update ring: self is gone
//   2. n.MigrateKeys()             — push keys to their new owners
//   3. server.Shutdown(ctx)        — stop accepting requests
func (n *Node) MigrateKeys() {
	keys := n.KV.Keys()
	migrated := 0

	for _, key := range keys {
		newOwner := n.Ring.GetNode(key)
		if newOwner == "" || newOwner == n.Addr {
			continue // ring is empty or still points to us (shouldn't happen after RemoveNode)
		}

		val, ok := n.KV.Get(key)
		if !ok {
			continue
		}

		if err := n.forwardPut(newOwner, key, val); err != nil {
			log.Printf("[MIGRATE] failed to migrate key=%q to %s: %v", key, newOwner, err)
		} else {
			n.KV.Delete(key)
			migrated++
		}
	}

	log.Printf("[MIGRATE] migrated %d/%d keys before shutdown", migrated, len(keys))
}

// forwardPut sends a PUT to another node's /forward/put endpoint.
// We use /forward/put (not /put) so the receiving node does LocalPut
// instead of another ring lookup — avoiding a potential second forward.
func (n *Node) forwardPut(owner, key, value string) error {
	body, err := json.Marshal(PutRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	resp, err := n.client.Post(
		"http://"+owner+"/forward/put",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("forward put to %s: %w", owner, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("forward put to %s returned %d", owner, resp.StatusCode)
	}
	return nil
}

// forwardGet sends a GET to another node's /forward/get endpoint.
func (n *Node) forwardGet(owner, key string) (string, bool, error) {
	resp, err := n.client.Get("http://" + owner + "/forward/get?key=" + key)
	if err != nil {
		return "", false, fmt.Errorf("forward get to %s: %w", owner, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("forward get to %s returned %d", owner, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("read response: %w", err)
	}

	var result GetResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", false, fmt.Errorf("unmarshal response: %w", err)
	}

	return result.Value, true, nil
}
