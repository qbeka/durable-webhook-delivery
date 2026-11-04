// Command loadgen sends a seeded event stream and prints a JSON summary.
//
// With --null-sink it answers 202 to everything instead. Pointing loadgen at a
// null sink measures the generator's own limit, separately from the service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
	"github.com/qbeka/durable-webhook-delivery/internal/workload"
)

func main() {
	apiURL := flag.String("api", "http://127.0.0.1:9000", "ingress base URL")
	tenant := flag.String("tenant", "", "tenant ID")
	source := flag.String("source", "", "source ID")
	secret := flag.String("secret", "", "source signing secret")
	eventType := flag.String("type", "receipt.forwarded", "event type")
	seed := flag.Uint64("seed", 1, "workload seed")
	events := flag.Int("events", 1000, "distinct events")
	rate := flag.Float64("rate", 100, "mean events per second")
	payloadBytes := flag.Int("payload-bytes", 1024, "body size")
	startUnixNs := flag.Int64("start-unix-ns", 0, "schedule start, 0 for one second from now")
	duplicates := flag.Int("duplicates", 0, "extra sends of already scheduled events")
	maxInFlight := flag.Int("max-in-flight", 1024, "most open requests")
	ackDeadline := flag.Duration("ack-deadline", 2*time.Minute, "how long one event may keep retrying")
	requestTimeout := flag.Duration("request-timeout", 5*time.Second, "timeout for one attempt")
	rawPath := flag.String("raw", "", "gzip CSV path for one row per send")
	summaryPath := flag.String("summary", "", "JSON summary path, empty for stdout")
	nullSink := flag.String("null-sink", "", "listen address, and answer 202 to everything")
	flag.Parse()

	if *nullSink != "" {
		log.Fatal(http.ListenAndServe(*nullSink, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		})))
	}

	if *startUnixNs == 0 {
		*startUnixNs = time.Now().Add(time.Second).UnixNano()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	summary, err := loadgen.Run(ctx, loadgen.Config{
		APIURL: *apiURL, Secret: *secret, EventType: *eventType,
		Stream: workload.Stream{
			TenantID: *tenant, SourceID: *source, Seed: *seed, EventCount: *events,
			RatePerSecond: *rate, PayloadBytes: *payloadBytes, StartUnixNs: *startUnixNs,
		},
		DuplicateCount: *duplicates, MaxInFlight: *maxInFlight, AckDeadline: *ackDeadline,
		RequestTimeout: *requestTimeout, RawOutputPath: *rawPath,
		RetryBackoff: 100 * time.Millisecond, RetryBackoffCap: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	output := os.Stdout
	if *summaryPath != "" {
		if output, err = os.Create(*summaryPath); err != nil {
			log.Fatal(err)
		}
		defer output.Close()
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil {
		log.Fatal(err)
	}
}
