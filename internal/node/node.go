package node

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/aadithyaa9/kv_store/internal/store"
)

type Node struct {
	Addr  string
	Peers []string
	KV    *store.KV
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func New(addr string, peers []string) *Node {
	return &Node{
		Addr:  addr,
		Peers: peers,
		KV:    store.New(),
	}
}

func (n *Node) Put(key, value string) {
	n.KV.Put(key, value)
	go n.replicate(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.KV.Get(key)
}

func (n *Node) replicate(key, value string) {
	reqBody, _ := json.Marshal(PutRequest{Key: key, Value: value})

	for _, peer := range n.Peers {
		url := "http://" + peer + "/replicate"

		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			log.Printf("[WARN] replication failed -> %s", peer)
			continue
		}
		resp.Body.Close()
	}
}