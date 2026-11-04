// Package ingress is the authenticated HTTP endpoint that accepts events.
package ingress

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// MaxBodyBytes is the largest event body accepted.
const MaxBodyBytes = 256 * 1024

// Handler serves POST /v1/sources/{source}/events.
type Handler struct {
	Store *store.Store
	Now   func() time.Time
}

// Routes returns the ingress API.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sources/{source}/events", h.receive)
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /metrics", h.metrics)
	return mux
}

type response struct {
	Status         string `json:"status"`
	EventID        string `json:"event_id,omitempty"`
	Deliveries     *int   `json:"deliveries,omitempty"`
	ReceivedSHA256 string `json:"received_sha256,omitempty"`
	StoredSHA256   string `json:"stored_sha256,omitempty"`
	Error          string `json:"error,omitempty"`
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("Hook-Tenant")
	sourceID := r.PathValue("source")
	eventID := r.Header.Get("Hook-Event-Id")
	eventType := r.Header.Get("Hook-Event-Type")
	for _, value := range []string{tenantID, sourceID, eventID, eventType} {
		if !signing.ValidID(value) {
			writeJSON(w, http.StatusBadRequest, response{Status: "rejected", Error: "missing or invalid identity header"})
			return
		}
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, response{Status: "rejected", Error: "body too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, response{Status: "rejected", Error: "could not read body"})
		return
	}

	unixSeconds, err := signing.ParseFreshTimestamp(r.Header.Get("Hook-Timestamp"), h.Now())
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, response{Status: "rejected", Error: "bad signature"})
		return
	}
	secret, err := h.Store.SourceSecret(r.Context(), tenantID, sourceID)
	if errors.Is(err, store.ErrUnknownSource) {
		// Same answer as a bad signature, so a caller cannot probe for sources.
		writeJSON(w, http.StatusUnauthorized, response{Status: "rejected", Error: "bad signature"})
		return
	}
	if err != nil {
		h.unavailable(w, r, err)
		return
	}
	digest := signing.BodyDigest(body)
	expected := signing.IngressSignature(secret, unixSeconds, tenantID, sourceID, eventID, eventType, digest)
	if !signing.Equal(expected, r.Header.Get("Hook-Signature")) {
		writeJSON(w, http.StatusUnauthorized, response{Status: "rejected", Error: "bad signature"})
		return
	}

	result, err := h.Store.Ingest(r.Context(), store.Event{
		TenantID: tenantID, SourceID: sourceID, EventID: eventID, EventType: eventType,
		Payload: body, PayloadSHA256: digest,
	})
	if err != nil {
		h.unavailable(w, r, err)
		return
	}

	// Everything below runs after COMMIT returned. A failure to write this
	// response leaves the event stored, and the sender's retry converges.
	switch result.Status {
	case store.IngestAccepted:
		writeJSON(w, http.StatusAccepted, response{Status: "accepted", EventID: eventID, Deliveries: &result.Deliveries})
	case store.IngestDuplicate:
		writeJSON(w, http.StatusOK, response{Status: "duplicate", EventID: eventID})
	case store.IngestConflict:
		writeJSON(w, http.StatusConflict, response{
			Status: "conflict", EventID: eventID,
			ReceivedSHA256: hex.EncodeToString(digest),
			StoredSHA256:   hex.EncodeToString(result.StoredSHA256),
		})
	}
}

// unavailable tells the sender to retry. The outcome may be unknown here: a
// connection can fail after COMMIT. Retrying with the same ID is always safe.
func (h *Handler) unavailable(w http.ResponseWriter, r *http.Request, err error) {
	if !errors.Is(err, context.Canceled) {
		slog.Warn("ingress database error", "path", r.URL.Path, "error", err)
	}
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, response{Status: "unavailable", Error: "database unavailable, retry with the same event ID"})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.Store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, response{Status: "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, response{Status: "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
