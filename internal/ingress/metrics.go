package ingress

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// metrics serves delivery state in the Prometheus text format. It reads the
// database on each scrape and keeps no counters in memory, so a restart loses
// nothing and every process reports the same truth.
func (h *Handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	statuses, err := h.Store.Status(ctx)
	if err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP hook_deliveries Deliveries by destination and state.")
	fmt.Fprintln(w, "# TYPE hook_deliveries gauge")
	for _, status := range statuses {
		labels := "destination=" + strconv.Quote(status.Name) + ",destination_id=\"" + strconv.FormatInt(status.DestinationID, 10) + "\""
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"pending\"} %d\n", labels, status.Pending)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"leased\"} %d\n", labels, status.Leased)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"succeeded\"} %d\n", labels, status.Succeeded)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"dead\"} %d\n", labels, status.Dead)
	}
	fmt.Fprintln(w, "# HELP hook_oldest_pending_seconds Age of the oldest pending delivery.")
	fmt.Fprintln(w, "# TYPE hook_oldest_pending_seconds gauge")
	for _, status := range statuses {
		fmt.Fprintf(w, "hook_oldest_pending_seconds{destination=%s} %.3f\n", strconv.Quote(status.Name), status.OldestPendingSecond)
	}
	fmt.Fprintln(w, "# HELP hook_breaker_open_seconds Seconds until a destination's breaker allows a probe.")
	fmt.Fprintln(w, "# TYPE hook_breaker_open_seconds gauge")
	for _, status := range statuses {
		fmt.Fprintf(w, "hook_breaker_open_seconds{destination=%s} %.3f\n", strconv.Quote(status.Name), status.BreakerOpenSeconds)
	}
}
