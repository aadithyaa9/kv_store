package transport

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/aadithyaa9/kv_store/internal/node"
)

type Handler struct {
	Node *node.Node
}

func NewHandler(n *node.Node) http.Handler {
	h := &Handler{Node: n}
	r := chi.NewRouter()

	// Client-facing
	r.Post("/put", h.put)
	r.Get("/get", h.get)

	// Node-to-node forwarding
	r.Post("/forward/put", h.forwardPut)
	r.Get("/forward/get", h.forwardGet)

	// Ring management
	r.Post("/nodes/add", h.addNode)
	r.Post("/nodes/remove", h.removeNode)

	// Observability
	r.Get("/ring", h.ringStatus)

	r.Get("/ring/positions", h.ringPositions)
	r.Get("/health", h.health)

	return r
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	var req node.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	if err := h.Node.Put(req.Key, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key query param required", http.StatusBadRequest)
		return
	}
	val, ok, err := h.Node.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node.GetResponse{Key: key, Value: val})
}

func (h *Handler) forwardPut(w http.ResponseWriter, r *http.Request) {
	var req node.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	h.Node.LocalPut(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) forwardGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	val, ok := h.Node.KV.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node.GetResponse{Key: key, Value: val})
}

func (h *Handler) addNode(w http.ResponseWriter, r *http.Request) {
	var req node.NodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Addr == "" {
		http.Error(w, "addr required", http.StatusBadRequest)
		return
	}
	h.Node.AddNode(req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) removeNode(w http.ResponseWriter, r *http.Request) {
	var req node.NodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Addr == "" {
		http.Error(w, "addr required", http.StatusBadRequest)
		return
	}
	h.Node.RemoveNode(req.Addr)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) ringStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"self":         h.Node.Addr,
		"nodes":        h.Node.Ring.Nodes(),
		"total_vnodes": h.Node.Ring.Size(),
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *Handler) ringPositions(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("node")
	positions := h.Node.Ring.PositionsFor(addr)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node":      addr,
		"count":     len(positions),
		"positions": positions,
	})
}