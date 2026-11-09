// Package api is mailq's HTTP interface.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/mail"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mariocandido/mailq/internal/metrics"
	"github.com/mariocandido/mailq/internal/smtpclient"
	"github.com/mariocandido/mailq/internal/store"
)

const (
	maxBodyBytes  = 1 << 20
	maxRecipients = 50
	maxKeyLen     = 255
)

// Server serves the API.
type Server struct {
	Store    *store.Store
	Metrics  *metrics.Metrics
	Gatherer prometheus.Gatherer
	Log      *slog.Logger
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", s.send)
	mux.HandleFunc("GET /messages/{id}", s.getMessage)
	mux.HandleFunc("GET /suppressions", s.listSuppressions)
	mux.HandleFunc("DELETE /suppressions/{recipient}", s.deleteSuppression)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	return mux
}

// SendRequest is the body of POST /send.
type SendRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}

type sentMessage struct {
	ID     string `json:"id"`
	To     string `json:"to"`
	Status string `json:"status"`
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req SendRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	msgs, err := Validate(req)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxKeyLen {
		writeError(w, http.StatusBadRequest, "Idempotency-Key too long")
		return
	}

	stored, err := s.Store.Enqueue(r.Context(), key, msgs)
	if err != nil {
		s.Log.Error("enqueue failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not enqueue")
		return
	}
	out := make([]sentMessage, len(stored))
	for i, m := range stored {
		s.Metrics.Enqueued.WithLabelValues(m.Status).Inc()
		out[i] = sentMessage{ID: m.ID, To: m.To, Status: m.Status}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"messages": out})
}

// Validate checks a request and expands it into one message per recipient.
func Validate(req SendRequest) ([]store.NewMessage, error) {
	from, err := mail.ParseAddress(req.From)
	if err != nil {
		return nil, fmt.Errorf("invalid from address: %v", err)
	}
	if len(req.To) == 0 || len(req.To) > maxRecipients {
		return nil, fmt.Errorf("to must have between 1 and %d recipients", maxRecipients)
	}
	if strings.ContainsAny(req.Subject, "\r\n") {
		return nil, errors.New("subject must not contain line breaks")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, errors.New("text is required")
	}

	seen := map[string]bool{}
	var out []store.NewMessage
	for _, raw := range req.To {
		a, err := mail.ParseAddress(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid recipient %q: %v", raw, err)
		}
		// Local parts are technically case-sensitive, but virtually every
		// provider treats them case-insensitively; normalising keeps
		// suppression and idempotency lookups consistent.
		addr := strings.ToLower(a.Address)
		if seen[addr] {
			continue
		}
		seen[addr] = true
		out = append(out, store.NewMessage{
			From: from.Address, To: addr, Domain: smtpclient.DomainOf(addr),
			Subject: req.Subject, Body: req.Text,
		})
	}
	return out, nil
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeError(w, http.StatusNotFound, "message not found")
		return
	}
	m, attempts, err := s.Store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "message not found")
		return
	}
	if err != nil {
		s.Log.Error("get failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load message")
		return
	}
	if attempts == nil {
		attempts = []store.Attempt{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": m, "attempts": attempts})
}

func (s *Server) listSuppressions(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Suppressions(r.Context(), 500)
	if err != nil {
		s.Log.Error("list suppressions failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list suppressions")
		return
	}
	if list == nil {
		list = []store.Suppression{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"suppressions": list})
}

func (s *Server) deleteSuppression(w http.ResponseWriter, r *http.Request) {
	ok, err := s.Store.DeleteSuppression(r.Context(), strings.ToLower(r.PathValue("recipient")))
	if err != nil {
		s.Log.Error("delete suppression failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not delete suppression")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "recipient is not suppressed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
