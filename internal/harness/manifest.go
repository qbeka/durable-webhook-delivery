package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Manifest is the machine-readable record of a run. With it and the commit, the
// run can be repeated and its numbers traced to their inputs.
type Manifest struct {
	Name           string            `json:"name"`
	Command        string            `json:"command"`
	StartedAt      time.Time         `json:"started_at"`
	ElapsedSeconds float64           `json:"elapsed_seconds"`
	Commit         string            `json:"commit"`
	WorkingTree    string            `json:"working_tree"` // clean, or dirty with a file list
	Seed           uint64            `json:"seed"`
	Configuration  any               `json:"configuration"`
	InputHashes    map[string]string `json:"input_sha256"`
	Machine        map[string]string `json:"machine"`
	Toolchain      map[string]string `json:"toolchain"`
	Database       map[string]string `json:"database_settings"`
	Files          map[string]string `json:"file_sha256,omitempty"`
	// The machine is shared, so a result must say how busy it was. Numbers from a
	// loaded machine are lower bounds on what the service can do alone.
	BackgroundLoad map[string]string `json:"background_load"`
}

var manifestSettings = []string{
	"server_version", "wal_sync_method", "fsync", "synchronous_commit", "full_page_writes",
	"shared_buffers", "wal_buffers", "max_wal_size", "checkpoint_timeout", "commit_delay",
	"autovacuum_naptime", "autovacuum_vacuum_cost_limit", "max_connections",
}

func command(name string, arguments ...string) string {
	output, err := exec.Command(name, arguments...).CombinedOutput()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return strings.TrimSpace(string(output))
}

// NewManifest records everything knowable before the run starts.
func NewManifest(ctx context.Context, root, name string, seed uint64, configuration any) (*Manifest, error) {
	manifest := &Manifest{
		Name: name, Command: strings.Join(os.Args, " "), StartedAt: time.Now(), Seed: seed,
		Configuration: configuration, InputHashes: map[string]string{},
		Commit: command("git", "-C", root, "rev-parse", "HEAD"),
		Machine: map[string]string{
			"cpu":              command("sysctl", "-n", "machdep.cpu.brand_string"),
			"logical_cpus":     command("sysctl", "-n", "hw.ncpu"),
			"performance_cpus": command("sysctl", "-n", "hw.perflevel0.physicalcpu"),
			"efficiency_cpus":  command("sysctl", "-n", "hw.perflevel1.physicalcpu"),
			"memory_bytes":     command("sysctl", "-n", "hw.memsize"),
			"os":               command("sw_vers", "-productVersion"),
			"storage":          "internal Apple SSD, APFS",
			"model":            command("sysctl", "-n", "hw.model"),
		},
		Toolchain: map[string]string{"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH},
		Database:  map[string]string{},
	}
	manifest.BackgroundLoad = map[string]string{
		"load_average_at_start": command("sysctl", "-n", "vm.loadavg"),
		"busiest_at_start":      busiestProcesses(),
	}
	manifest.WorkingTree = "clean"
	if status := command("git", "-C", root, "status", "--porcelain"); status != "" {
		manifest.WorkingTree = "dirty: " + strings.ReplaceAll(status, "\n", "; ")
	}
	for _, path := range []string{"config/postgresql.conf", "internal/store/schema.sql", "internal/receiver/schema.sql", "go.sum", "internal/workload/workload.go"} {
		digest, err := fileDigest(filepath.Join(root, path))
		if err != nil {
			return nil, err
		}
		manifest.InputHashes[path] = digest
	}
	connection, err := pgx.Connect(ctx, EngineURL)
	if err != nil {
		return nil, err
	}
	defer connection.Close(ctx)
	for _, setting := range manifestSettings {
		var value string
		if err := connection.QueryRow(ctx, "SHOW "+setting).Scan(&value); err != nil {
			return nil, fmt.Errorf("read %s: %w", setting, err)
		}
		manifest.Database[setting] = value
	}
	return manifest, nil
}

// Finish stamps the elapsed time, hashes the raw files and writes the manifest.
func (m *Manifest) Finish(directory string) error {
	m.ElapsedSeconds = time.Since(m.StartedAt).Seconds()
	m.BackgroundLoad["load_average_at_end"] = command("sysctl", "-n", "vm.loadavg")
	m.Files = map[string]string{}
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() == "manifest.json" {
			return err
		}
		digest, err := fileDigest(path)
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(directory, path)
		m.Files[relative] = digest
		return nil
	})
	if err != nil {
		return err
	}
	return writeJSON(filepath.Join(directory, "manifest.json"), m)
}

// busiestProcesses names the top CPU users outside this project. Only the program
// name is kept, so no command line or path from another project is recorded.
func busiestProcesses() string {
	output := command("ps", "-Aco", "pcpu=,comm=", "-r")
	lines := strings.Split(output, "\n")
	return strings.Join(lines[:min(5, len(lines))], "; ")
}

// QuietGate waits for other work on the machine before a repetition starts. The
// machine is shared, and this project does not stop other people's processes.
type QuietGate struct {
	MaxCPUPercent float64       // 100 is one core
	MaxWait       time.Duration // run anyway after this, and say so
}

// GateResult records the conditions a repetition really ran under.
type GateResult struct {
	BackgroundCPUPercent float64 `json:"background_cpu_percent"`
	WaitedSeconds        float64 `json:"waited_seconds"`
	Contended            bool    `json:"contended"`
}

// Wait returns once the machine is quiet enough or MaxWait has passed.
func (g QuietGate) Wait(ctx context.Context) GateResult {
	started := time.Now()
	for {
		percent := backgroundCPUPercent()
		quiet := percent <= g.MaxCPUPercent
		if quiet || time.Since(started) >= g.MaxWait || ctx.Err() != nil {
			return GateResult{percent, time.Since(started).Seconds(), !quiet}
		}
		time.Sleep(15 * time.Second)
	}
}

func backgroundCPUPercent() float64 {
	var total float64
	for _, line := range strings.Split(command("ps", "-Aco", "pcpu="), "\n") {
		value, err := strconv.ParseFloat(strings.TrimSpace(line), 64)
		if err == nil {
			total += value
		}
	}
	return total
}

func fileDigest(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), nil
}
