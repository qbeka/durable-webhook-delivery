package failpoint

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	parsed, err := parse("lease_stall@3=250ms")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.name != "lease_stall" || parsed.targetHit != 3 || parsed.sleep != 250*time.Millisecond {
		t.Fatalf("unexpected rule: %+v", parsed)
	}
	for _, bad := range []string{"", "name", "@3", "name@0", "name@x", "name@1=", "name@1=-1s"} {
		if _, err := parse(bad); err == nil {
			t.Errorf("%q should not parse", bad)
		}
	}
}

func TestHitSleepsOnlyOnTheTargetHit(t *testing.T) {
	parsed, err := parse("stall@2=30ms")
	if err != nil {
		t.Fatal(err)
	}
	active.Store(parsed)
	defer active.Store(nil)

	start := time.Now()
	Hit("other")
	Hit("stall")
	if time.Since(start) >= 30*time.Millisecond {
		t.Fatal("slept before the target hit")
	}
	Hit("stall")
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("did not sleep on the target hit")
	}
}
