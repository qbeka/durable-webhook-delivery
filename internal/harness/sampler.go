package harness

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/qbeka/durable-webhook-delivery/internal/store"
)

// DepthSample is one queue depth reading.
type DepthSample struct {
	Time          time.Time
	DestinationID int64
	Pending       int64
	Leased        int64
}

// sampleQueue records depth once a second until ctx ends. A failed reading during
// a database outage is skipped: the gap in the file shows the outage.
func sampleQueue(ctx context.Context, database *store.Store, routes []Route, path string) ([]DepthSample, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	_ = writer.Write([]string{"time_unix_ms", "destination_id", "pending", "leased"})

	var samples []DepthSample
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return samples, nil
		case now := <-ticker.C:
			readContext, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			depths, err := database.QueueDepths(readContext)
			cancel()
			if err != nil {
				continue
			}
			byDestination := make(map[int64]store.QueueDepth, len(depths))
			for _, depth := range depths {
				byDestination[depth.DestinationID] = depth
			}
			// A destination with no row has zero depth. Record it explicitly.
			for _, route := range routes {
				depth := byDestination[route.DestinationID]
				samples = append(samples, DepthSample{now, route.DestinationID, depth.Pending, depth.Leased})
				_ = writer.Write([]string{
					strconv.FormatInt(now.UnixMilli(), 10), fmt.Sprint(route.DestinationID),
					strconv.FormatInt(depth.Pending, 10), strconv.FormatInt(depth.Leased, 10),
				})
			}
			writer.Flush()
		}
	}
}
