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

	r.Post("/put", h.put)
	r.Get("/get", h.get)
	r.Post("/replicate", h.replicate)
	r.Get("/health", h.health)

	return r
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	var req node.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	h.Node.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	val, ok := h.Node.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": val,
	})
}

func (h *Handler) replicate(w http.ResponseWriter, r *http.Request) {
	var req node.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	h.Node.KV.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}