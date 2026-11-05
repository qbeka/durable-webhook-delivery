package ingress

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// labelEscaper applies the only three escapes the Prometheus text format allows.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func labels(status store.DestinationStatus) string {
	return fmt.Sprintf(`tenant="%s",destination="%s",destination_id="%s"`,
		labelEscaper.Replace(status.TenantID), labelEscaper.Replace(status.Name),
		strconv.FormatInt(status.DestinationID, 10))
}

// metrics serves delivery state in the Prometheus text format. It reads the
// database on each scrape and keeps no counters in memory, so a restart loses
// nothing and every process reports the same truth. Like /healthz it has no
// authentication, so keep it on a private network.
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
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"pending\"} %d\n", labels(status), status.Pending)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"leased\"} %d\n", labels(status), status.Leased)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"succeeded\"} %d\n", labels(status), status.Succeeded)
		fmt.Fprintf(w, "hook_deliveries{%s,state=\"dead\"} %d\n", labels(status), status.Dead)
	}
	fmt.Fprintln(w, "# HELP hook_oldest_pending_seconds Age of the oldest pending delivery.")
	fmt.Fprintln(w, "# TYPE hook_oldest_pending_seconds gauge")
	for _, status := range statuses {
		fmt.Fprintf(w, "hook_oldest_pending_seconds{%s} %.3f\n", labels(status), status.OldestPendingSecond)
	}
	fmt.Fprintln(w, "# HELP hook_breaker_open_seconds Seconds until a destination's breaker allows a probe.")
	fmt.Fprintln(w, "# TYPE hook_breaker_open_seconds gauge")
	for _, status := range statuses {
		fmt.Fprintf(w, "hook_breaker_open_seconds{%s} %.3f\n", labels(status), status.BreakerOpenSeconds)
	}
}
