package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/qbeka/durable-webhook-delivery/internal/loadgen"
	"github.com/qbeka/durable-webhook-delivery/internal/receiver"
	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

const (
	EngineURL    = "postgres://hook@127.0.0.1:54330/hookd"
	ReceiverURL  = "postgres://hook@127.0.0.1:54331/receipts"
	apiAddress   = "127.0.0.1:9000"
	sourceSecret = "whsec_synthetic_source"
	destSecret   = "whsec_synthetic_destination"
)

// Options sets the size and timing of a cluster. Timings are short so a fault
// run finishes in minutes. They are declared in every result file.
type Options struct {
	Workers              int           `json:"workers"`
	WorkerConcurrency    int           `json:"worker_concurrency"`
	WorkerMaxConnections int           `json:"worker_max_connections"`
	Lease                time.Duration `json:"lease_ns"`
	LeaseMargin          time.Duration `json:"lease_margin_ns"`
	RequestTimeout       time.Duration `json:"request_timeout_ns"`
	PollInterval         time.Duration `json:"poll_interval_ns"`
	ReapInterval         time.Duration `json:"reap_interval_ns"`
	MaxAttempts          int           `json:"max_attempts"`
	RetryBase            time.Duration `json:"retry_base_ns"`
	RetryCap             time.Duration `json:"retry_cap_ns"`
	BreakerThreshold     int           `json:"breaker_threshold"`
	BreakerBase          time.Duration `json:"breaker_base_ns"`
	BreakerCap           time.Duration `json:"breaker_cap_ns"`
	MaxInFlight          int           `json:"max_in_flight"` // per destination, across workers
}

// FaultOptions are the timings used by the fault scenarios.
func FaultOptions() Options {
	return Options{
		Workers: 3, WorkerConcurrency: 32, WorkerMaxConnections: 16,
		Lease: 3 * time.Second, LeaseMargin: 500 * time.Millisecond, RequestTimeout: time.Second,
		PollInterval: 10 * time.Millisecond, ReapInterval: 500 * time.Millisecond,
		MaxAttempts: 60, RetryBase: 100 * time.Millisecond, RetryCap: 2 * time.Second,
		BreakerThreshold: 8, BreakerBase: 200 * time.Millisecond, BreakerCap: 2 * time.Second,
		MaxInFlight: 64,
	}
}

// Route is one tenant's source and destination, served by its own receiver.
type Route struct {
	Tenant        string
	Source        string
	Destination   string
	ReceiverAddr  string
	AdminAddr     string
	ReceiverMode  receiver.Mode
	DestinationID int64
}

// HealthyRoute carries the traffic that must stay fast.
func HealthyRoute(mode receiver.Mode) Route {
	return Route{Tenant: "healthy", Source: "mail", Destination: "ledger",
		ReceiverAddr: "127.0.0.1:9101", AdminAddr: "127.0.0.1:9201", ReceiverMode: mode}
}

// FlakyRoute carries the traffic whose destination fails.
func FlakyRoute(mode receiver.Mode) Route {
	return Route{Tenant: "flaky", Source: "mail", Destination: "ledger",
		ReceiverAddr: "127.0.0.1:9102", AdminAddr: "127.0.0.1:9202", ReceiverMode: mode}
}

// Cluster is one running system under test.
type Cluster struct {
	Root       string // repository root
	RunDir     string
	Options    Options
	Routes     []Route
	Store      *store.Store
	api        *Process
	mutex      sync.Mutex
	workers    []*Process
	workerSeq  int
	receivers  map[string]*Process
	loadgens   []*Process
	failpoints map[int]string
}

// NewCluster resets both databases and starts the API, receivers and workers.
func NewCluster(ctx context.Context, root, runDir string, options Options, routes []Route) (*Cluster, error) {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, err
	}
	c := &Cluster{
		Root: root, RunDir: runDir, Options: options, Routes: routes,
		receivers: map[string]*Process{}, failpoints: map[int]string{},
		workers: make([]*Process, options.Workers),
	}
	if err := c.resetDatabases(ctx); err != nil {
		return nil, err
	}
	var err error
	if c.Store, err = store.Open(ctx, EngineURL, 4); err != nil {
		return nil, err
	}
	if err := c.Store.Migrate(ctx); err != nil {
		return nil, err
	}
	if err := c.registerRoutes(ctx); err != nil {
		return nil, err
	}
	if err := c.StartAPI(ctx); err != nil {
		return nil, err
	}
	for _, route := range c.Routes {
		if err := c.StartReceiver(ctx, route); err != nil {
			return nil, err
		}
	}
	for slot := range c.workers {
		if err := c.StartWorker(slot, ""); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) binary(name string) string {
	return filepath.Join(c.Root, "bin", name)
}

func (c *Cluster) resetDatabases(ctx context.Context) error {
	statements := map[string]string{
		EngineURL:   `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`,
		ReceiverURL: `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`,
	}
	for url, statement := range statements {
		connection, err := pgx.Connect(ctx, url)
		if err != nil {
			return fmt.Errorf("connect, is `make up` running? %w", err)
		}
		_, err = connection.Exec(ctx, statement)
		connection.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) registerRoutes(ctx context.Context) error {
	for index := range c.Routes {
		route := &c.Routes[index]
		if err := c.Store.PutSource(ctx, route.Tenant, route.Source, sourceSecret); err != nil {
			return err
		}
		id, err := c.Store.PutDestination(ctx, store.Destination{
			TenantID: route.Tenant, Name: route.Destination,
			URL:    "http://" + route.ReceiverAddr + "/webhooks/receipts",
			Secret: destSecret, MaxInFlight: c.Options.MaxInFlight,
		})
		if err != nil {
			return err
		}
		route.DestinationID = id
		if err := c.Store.Subscribe(ctx, route.Tenant, route.Source, "*", id); err != nil {
			return err
		}
	}
	return nil
}

// StartAPI starts the ingress server and waits until it answers.
func (c *Cluster) StartAPI(ctx context.Context) error {
	var err error
	c.api, err = startProcess(c.RunDir, "api", c.binary("hookd"), []string{"api", "--addr", apiAddress}, nil)
	if err != nil {
		return err
	}
	return waitForHTTP(ctx, "http://"+apiAddress+"/healthz")
}

// StartReceiver starts a route's receiver and waits until it answers.
func (c *Cluster) StartReceiver(ctx context.Context, route Route) error {
	name := "receiver-" + route.Tenant
	process, err := startProcess(c.RunDir, name+"-"+strconv.FormatInt(time.Now().UnixNano(), 36), c.binary("receiver"), []string{
		"--addr", route.ReceiverAddr, "--admin-addr", route.AdminAddr, "--secret", destSecret,
		"--mode", string(route.ReceiverMode), "--effect-log", c.EffectLogPath(route),
	}, nil)
	if err != nil {
		return err
	}
	c.mutex.Lock()
	c.receivers[route.Tenant] = process
	c.mutex.Unlock()
	return waitForHTTP(ctx, "http://"+route.ReceiverAddr+"/healthz")
}

// EffectLogPath is where a route's receiver writes its per-request rows.
func (c *Cluster) EffectLogPath(route Route) string {
	return filepath.Join(c.RunDir, "effects-"+route.Tenant+".csv")
}

// KillReceiver stops a route's receiver with SIGKILL.
func (c *Cluster) KillReceiver(route Route) {
	c.mutex.Lock()
	process := c.receivers[route.Tenant]
	c.mutex.Unlock()
	if process != nil {
		process.Kill()
	}
}

// Receiver returns a route's running receiver process.
func (c *Cluster) Receiver(route Route) *Process {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.receivers[route.Tenant]
}

// StartWorker fills a worker slot. A nonempty failpoint arms the new process.
func (c *Cluster) StartWorker(slot int, failpoint string) error {
	c.mutex.Lock()
	c.workerSeq++
	sequence := c.workerSeq
	c.mutex.Unlock()

	options := c.Options
	id := fmt.Sprintf("worker-%d", sequence)
	environment := []string{}
	if failpoint != "" {
		environment = append(environment, "HOOK_FAILPOINT="+failpoint)
	}
	process, err := startProcess(c.RunDir, id, c.binary("hookd"), []string{
		"worker", "--id", id,
		"--concurrency", strconv.Itoa(options.WorkerConcurrency),
		"--max-connections", strconv.Itoa(options.WorkerMaxConnections),
		"--lease", options.Lease.String(), "--lease-margin", options.LeaseMargin.String(),
		"--request-timeout", options.RequestTimeout.String(),
		"--poll-interval", options.PollInterval.String(), "--reap-interval", options.ReapInterval.String(),
		"--max-attempts", strconv.Itoa(options.MaxAttempts),
		"--retry-base", options.RetryBase.String(), "--retry-cap", options.RetryCap.String(),
		"--breaker-threshold", strconv.Itoa(options.BreakerThreshold),
		"--breaker-base", options.BreakerBase.String(), "--breaker-cap", options.BreakerCap.String(),
	}, environment)
	if err != nil {
		return err
	}
	c.mutex.Lock()
	c.workers[slot] = process
	c.mutex.Unlock()
	return nil
}

// Worker returns the process in a slot.
func (c *Cluster) Worker(slot int) *Process {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.workers[slot]
}

// SetFaults replaces a route's receiver fault plan.
func (c *Cluster) SetFaults(route Route, plan receiver.FaultPlan) error {
	body, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	response, err := http.Post("http://"+route.AdminAddr+"/faults", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("set faults: status %d", response.StatusCode)
	}
	return nil
}

// PostgresCommand runs scripts/pg.sh, for example to crash a cluster.
func (c *Cluster) PostgresCommand(arguments ...string) error {
	output, err := exec.Command(filepath.Join(c.Root, "scripts/pg.sh"), arguments...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg.sh %v: %w: %s", arguments, err, output)
	}
	return nil
}

// WaitForDrain waits until nothing is pending or leased. It returns the seconds
// waited and whether the queue emptied before the deadline.
func (c *Cluster) WaitForDrain(ctx context.Context, deadline time.Duration) (float64, bool) {
	started := time.Now()
	for time.Since(started) < deadline {
		depths, err := c.Store.QueueDepths(ctx)
		if err == nil && len(depths) == 0 {
			return time.Since(started).Seconds(), true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return time.Since(started).Seconds(), false
}

// StopAll stops every process and closes the database connection.
func (c *Cluster) StopAll() {
	c.mutex.Lock()
	processes := append([]*Process{c.api}, c.workers...)
	for _, process := range c.receivers {
		processes = append(processes, process)
	}
	processes = append(processes, c.loadgens...)
	c.mutex.Unlock()
	var group sync.WaitGroup
	for _, process := range processes {
		if process == nil || process.Exited() {
			continue
		}
		_ = process.Resume() // a paused process cannot handle SIGTERM
		group.Add(1)
		go func() {
			defer group.Done()
			process.Terminate(5 * time.Second)
		}()
	}
	group.Wait()
	if c.Store != nil {
		c.Store.Close()
	}
}

// LoadPlan describes one load generator process.
type LoadPlan struct {
	Route        Route
	Seed         uint64
	Events       int
	Rate         float64
	Duplicates   int
	PayloadBytes int
	StartUnixNs  int64
	AckDeadline  time.Duration
}

// LoadRun is a running generator.
type LoadRun struct {
	Plan        LoadPlan
	summaryPath string
	process     *Process
}

// StartLoad launches a generator as its own process, so its CPU use cannot be
// confused with the service's.
func (c *Cluster) StartLoad(plan LoadPlan) (*LoadRun, error) {
	name := fmt.Sprintf("loadgen-%s-%d", plan.Route.Tenant, plan.Seed)
	summaryPath := filepath.Join(c.RunDir, name+".summary.json")
	process, err := startProcess(c.RunDir, name, c.binary("loadgen"), []string{
		"--api", "http://" + apiAddress, "--tenant", plan.Route.Tenant, "--source", plan.Route.Source,
		"--secret", sourceSecret, "--seed", strconv.FormatUint(plan.Seed, 10),
		"--events", strconv.Itoa(plan.Events), "--rate", strconv.FormatFloat(plan.Rate, 'f', -1, 64),
		"--payload-bytes", strconv.Itoa(plan.PayloadBytes),
		"--start-unix-ns", strconv.FormatInt(plan.StartUnixNs, 10),
		"--duplicates", strconv.Itoa(plan.Duplicates),
		"--ack-deadline", plan.AckDeadline.String(),
		"--raw", filepath.Join(c.RunDir, name+".sends.csv.gz"), "--summary", summaryPath,
	}, nil)
	if err != nil {
		return nil, err
	}
	c.mutex.Lock()
	c.loadgens = append(c.loadgens, process)
	c.mutex.Unlock()
	return &LoadRun{Plan: plan, summaryPath: summaryPath, process: process}, nil
}

// Wait returns the generator's summary when it finishes.
func (l *LoadRun) Wait(ctx context.Context) (loadgen.Summary, error) {
	if !l.process.WaitForExit(ctx) {
		return loadgen.Summary{}, fmt.Errorf("load generator did not finish: %w", ctx.Err())
	}
	var summary loadgen.Summary
	content, err := os.ReadFile(l.summaryPath)
	if err != nil {
		return summary, err
	}
	return summary, json.Unmarshal(content, &summary)
}

func waitForHTTP(ctx context.Context, url string) error {
	client := http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		response, err := client.Get(url)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s did not become ready", url)
}
