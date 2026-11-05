package ingress

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
	"github.com/qbeka/durable-webhook-delivery/internal/testdb"
)

const testSecret = "whsec_source"

func startIngress(t *testing.T) *httptest.Server {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, testdb.Engine(t), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.PutSource(ctx, "tenant", "source", testSecret); err != nil {
		t.Fatal(err)
	}
	handler := &Handler{Store: database, Now: time.Now}
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return server
}

type signedRequest struct {
	tenant, source, eventID, eventType, secret string
	body                                       []byte
	signedAt                                   time.Time
}

func goodRequest() signedRequest {
	return signedRequest{
		tenant: "tenant", source: "source", eventID: "evt-1", eventType: "receipt.forwarded",
		secret: testSecret, body: []byte(`{"a":1}`), signedAt: time.Now(),
	}
}

func (r signedRequest) send(t *testing.T, server *httptest.Server) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/sources/"+r.source+"/events", bytes.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	unixSeconds := r.signedAt.Unix()
	request.Header.Set("Hook-Tenant", r.tenant)
	request.Header.Set("Hook-Event-Id", r.eventID)
	request.Header.Set("Hook-Event-Type", r.eventType)
	request.Header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	request.Header.Set("Hook-Signature", signing.IngressSignature(r.secret, unixSeconds, r.tenant, r.source, r.eventID, r.eventType, signing.BodyDigest(r.body)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestIngressStatusCodes(t *testing.T) {
	server := startIngress(t)

	if got := goodRequest().send(t, server); got != http.StatusAccepted {
		t.Fatalf("first send: %d", got)
	}
	if got := goodRequest().send(t, server); got != http.StatusOK {
		t.Fatalf("same event again: %d", got)
	}
	conflicting := goodRequest()
	conflicting.body = []byte(`{"a":2}`)
	if got := conflicting.send(t, server); got != http.StatusConflict {
		t.Fatalf("different body: %d", got)
	}

	cases := []struct {
		name   string
		change func(*signedRequest)
		want   int
	}{
		{"wrong secret", func(r *signedRequest) { r.secret = "wrong" }, http.StatusUnauthorized},
		{"unknown source looks like a bad signature", func(r *signedRequest) { r.source = "nobody" }, http.StatusUnauthorized},
		{"old timestamp", func(r *signedRequest) { r.signedAt = time.Now().Add(-time.Hour) }, http.StatusUnauthorized},
		{"invalid event ID", func(r *signedRequest) { r.eventID = "has space" }, http.StatusBadRequest},
		{"too large", func(r *signedRequest) { r.body = []byte(strings.Repeat("x", MaxBodyBytes+1)) }, http.StatusRequestEntityTooLarge},
	}
	for _, testCase := range cases {
		request := goodRequest()
		request.eventID = "evt-2"
		testCase.change(&request)
		if got := request.send(t, server); got != testCase.want {
			t.Errorf("%s: got %d, want %d", testCase.name, got, testCase.want)
		}
	}
}

func TestDatabaseFailureAsksTheSenderToRetry(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, testdb.Engine(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	database.Close() // every query now fails
	handler := &Handler{Store: database, Now: time.Now}
	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	request := goodRequest()
	httpRequest, err := http.NewRequest(http.MethodPost, server.URL+"/v1/sources/source/events", bytes.NewReader(request.body))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Hook-Tenant", "tenant")
	httpRequest.Header.Set("Hook-Event-Id", "evt")
	httpRequest.Header.Set("Hook-Event-Type", "type")
	httpRequest.Header.Set("Hook-Timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	response, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d, Retry-After %q", response.StatusCode, response.Header.Get("Retry-After"))
	}
}

func TestMetricsAreUniquePerDestinationAndEscaped(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, testdb.Engine(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"one", "two"} {
		if _, err := database.PutDestination(ctx, store.Destination{
			TenantID: tenant, Name: "tab\tname", URL: "http://x/", Secret: "s", MaxInFlight: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	handler := &Handler{Store: database, Now: time.Now}
	server := httptest.NewServer(handler.Routes())
	defer server.Close()
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series := line[:strings.LastIndex(line, " ")]
		if seen[series] {
			t.Fatalf("duplicate series: %s", series)
		}
		seen[series] = true
		if strings.Contains(line, `\t`) {
			t.Fatalf("Prometheus allows only three escapes: %s", line)
		}
	}
}

func TestMetricsReportDeliveriesInPrometheusFormat(t *testing.T) {
	server := startIngress(t)
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "# TYPE hook_deliveries gauge") {
		t.Fatalf("%d: %s", response.StatusCode, body)
	}
}

func TestSignatureCoversTheEventIdentity(t *testing.T) {
	server := startIngress(t)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/sources/source/events", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	unixSeconds := time.Now().Unix()
	signature := signing.IngressSignature(testSecret, unixSeconds, "tenant", "source", "evt-original", "receipt.forwarded", signing.BodyDigest([]byte("{}")))
	request.Header.Set("Hook-Tenant", "tenant")
	request.Header.Set("Hook-Event-Id", "evt-changed-by-an-attacker")
	request.Header.Set("Hook-Event-Type", "receipt.forwarded")
	request.Header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	request.Header.Set("Hook-Signature", signature)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a body signed for one event was accepted as another: %d", response.StatusCode)
	}
}
