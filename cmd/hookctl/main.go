// Command hookctl is the operator CLI. It talks to the engine database directly.
//
// Run hookctl with no arguments for the command list.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/signing"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

const usage = `usage: hookctl <command> [flags]

Setup:
  migrate           create the engine tables
  source            register a source and its signing secret
  destination       register a destination and print its ID
  subscribe         route events from a source to a destination
  unsubscribe       stop routing future events
  pause, resume     hold or release a destination's deliveries

Operate:
  status            show delivery counts per destination
  trace             show one delivery with its full attempt history
  replay            send finished deliveries again under the same keys
  prune             apply payload and identity retention
  send              sign and send one event to the ingress API
`

const defaultDatabaseURL = "postgres://hook@127.0.0.1:54330/hookd"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	commands := map[string]func(context.Context, []string) error{
		"migrate": migrate, "source": source, "destination": destination,
		"subscribe": subscribe(true), "unsubscribe": subscribe(false),
		"pause": setPaused(true), "resume": setPaused(false),
		"status": status, "trace": trace, "replay": replay, "prune": prune, "send": send,
	}
	command, found := commands[os.Args[1]]
	if !found {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := command(context.Background(), os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "hookctl:", err)
		os.Exit(1)
	}
}

// commandFlags is a flag set with the database flag every command shares.
func commandFlags(name string) (*flag.FlagSet, *string) {
	flags := flag.NewFlagSet(name, flag.ExitOnError)
	return flags, flags.String("db", defaultDatabaseURL, "engine database URL")
}

func openStore(ctx context.Context, databaseURL string) (*store.Store, error) {
	return store.Open(ctx, databaseURL, 2)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func migrate(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("migrate")
	_ = flags.Parse(arguments)
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	return database.Migrate(ctx)
}

func source(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("source")
	tenant := flags.String("tenant", "", "tenant ID")
	sourceID := flags.String("source", "", "source ID")
	secret := flags.String("secret", "", "signing secret")
	_ = flags.Parse(arguments)
	if *tenant == "" || *sourceID == "" || *secret == "" {
		return errors.New("source needs --tenant, --source and --secret")
	}
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	return database.PutSource(ctx, *tenant, *sourceID, *secret)
}

func destination(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("destination")
	tenant := flags.String("tenant", "", "tenant ID")
	name := flags.String("name", "", "destination name, unique in the tenant")
	url := flags.String("url", "", "delivery URL")
	secret := flags.String("secret", "", "signing secret")
	maxInFlight := flags.Int("max-in-flight", 16, "most requests in flight across all workers")
	rate := flags.Float64("rate", 0, "most attempts per second, 0 for no limit")
	_ = flags.Parse(arguments)
	if *tenant == "" || *name == "" || *url == "" || *secret == "" {
		return errors.New("destination needs --tenant, --name, --url and --secret")
	}
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	id, err := database.PutDestination(ctx, store.Destination{
		TenantID: *tenant, Name: *name, URL: *url, Secret: *secret, MaxInFlight: *maxInFlight, RatePerSecond: *rate,
	})
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}

func subscribe(active bool) func(context.Context, []string) error {
	return func(ctx context.Context, arguments []string) error {
		flags, databaseURL := commandFlags("subscribe")
		tenant := flags.String("tenant", "", "tenant ID")
		sourceID := flags.String("source", "", "source ID")
		eventType := flags.String("type", "*", "event type, or * for all")
		destinationID := flags.Int64("destination", 0, "destination ID")
		_ = flags.Parse(arguments)
		if *tenant == "" || *sourceID == "" || *destinationID == 0 {
			return errors.New("needs --tenant, --source and --destination")
		}
		database, err := openStore(ctx, *databaseURL)
		if err != nil {
			return err
		}
		defer database.Close()
		if active {
			return database.Subscribe(ctx, *tenant, *sourceID, *eventType, *destinationID)
		}
		return database.Unsubscribe(ctx, *tenant, *sourceID, *eventType, *destinationID)
	}
}

func setPaused(paused bool) func(context.Context, []string) error {
	return func(ctx context.Context, arguments []string) error {
		flags, databaseURL := commandFlags("pause")
		destinationID := flags.Int64("destination", 0, "destination ID")
		_ = flags.Parse(arguments)
		if *destinationID == 0 {
			return errors.New("needs --destination")
		}
		database, err := openStore(ctx, *databaseURL)
		if err != nil {
			return err
		}
		defer database.Close()
		return database.SetDestinationPaused(ctx, *destinationID, paused)
	}
}

func status(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("status")
	_ = flags.Parse(arguments)
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	statuses, err := database.Status(ctx)
	if err != nil {
		return err
	}
	return printJSON(statuses)
}

func trace(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("trace")
	key := flags.String("key", "", "delivery key")
	_ = flags.Parse(arguments)
	if *key == "" {
		return errors.New("trace needs --key")
	}
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	record, err := database.DeliveryHistory(ctx, *key)
	if err != nil {
		return err
	}
	return printJSON(record)
}

func replay(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("replay")
	operator := flags.String("operator", os.Getenv("USER"), "who is replaying")
	reason := flags.String("reason", "", "why, stored in the audit log")
	key := flags.String("key", "", "one delivery key")
	destinationID := flags.Int64("destination", 0, "all matching deliveries of one destination")
	state := flags.String("state", store.StateDead, "dead or succeeded, with --destination")
	limit := flags.Int("limit", 0, "most deliveries to requeue, required")
	dryRun := flags.Bool("dry-run", false, "count matches and change nothing")
	_ = flags.Parse(arguments)
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Replay(ctx, store.ReplayRequest{
		Operator: *operator, Reason: *reason, DeliveryKey: *key, DestinationID: *destinationID,
		State: *state, Limit: *limit, DryRun: *dryRun,
	})
	if err != nil {
		return err
	}
	return printJSON(result)
}

func prune(ctx context.Context, arguments []string) error {
	flags, databaseURL := commandFlags("prune")
	payloadRetention := flags.Duration("payload-retention", 7*24*time.Hour, "clear payloads older than this")
	identityRetention := flags.Duration("identity-retention", 30*24*time.Hour, "delete events older than this")
	_ = flags.Parse(arguments)
	if *identityRetention < *payloadRetention {
		return errors.New("identity retention must not be shorter than payload retention")
	}
	database, err := openStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Prune(ctx, *payloadRetention, *identityRetention)
	if err != nil {
		return err
	}
	return printJSON(result)
}

func send(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("send", flag.ExitOnError)
	apiURL := flags.String("api", "http://127.0.0.1:9000", "ingress base URL")
	tenant := flags.String("tenant", "", "tenant ID")
	sourceID := flags.String("source", "", "source ID")
	secret := flags.String("secret", "", "source signing secret")
	eventID := flags.String("event-id", "", "event ID")
	eventType := flags.String("type", "receipt.forwarded", "event type")
	body := flags.String("body", "", "request body")
	_ = flags.Parse(arguments)
	if *tenant == "" || *sourceID == "" || *secret == "" || *eventID == "" || *body == "" {
		return errors.New("send needs --tenant, --source, --secret, --event-id and --body")
	}

	url := fmt.Sprintf("%s/v1/sources/%s/events", *apiURL, *sourceID)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(*body))
	if err != nil {
		return err
	}
	unixSeconds := time.Now().Unix()
	digest := signing.BodyDigest([]byte(*body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Hook-Tenant", *tenant)
	request.Header.Set("Hook-Event-Id", *eventID)
	request.Header.Set("Hook-Event-Type", *eventType)
	request.Header.Set("Hook-Timestamp", strconv.FormatInt(unixSeconds, 10))
	request.Header.Set("Hook-Signature", signing.IngressSignature(*secret, unixSeconds, *tenant, *sourceID, *eventID, *eventType, digest))

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	fmt.Printf("%d %s", response.StatusCode, answer)
	return nil
}
