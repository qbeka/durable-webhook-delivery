package signing

import (
	"strconv"
	"testing"
	"time"
)

func TestIngressSignatureBindsEveryIdentityField(t *testing.T) {
	digest := BodyDigest([]byte(`{"a":1}`))
	base := IngressSignature("secret", 100, "tenant", "source", "evt.1", "receipt.forwarded", digest)

	variants := map[string]string{
		"secret":    IngressSignature("other", 100, "tenant", "source", "evt.1", "receipt.forwarded", digest),
		"timestamp": IngressSignature("secret", 101, "tenant", "source", "evt.1", "receipt.forwarded", digest),
		"tenant":    IngressSignature("secret", 100, "tenant2", "source", "evt.1", "receipt.forwarded", digest),
		"source":    IngressSignature("secret", 100, "tenant", "source2", "evt.1", "receipt.forwarded", digest),
		"event":     IngressSignature("secret", 100, "tenant", "source", "evt.2", "receipt.forwarded", digest),
		"type":      IngressSignature("secret", 100, "tenant", "source", "evt.1", "receipt.other", digest),
		"body":      IngressSignature("secret", 100, "tenant", "source", "evt.1", "receipt.forwarded", BodyDigest([]byte(`{"a":2}`))),
	}
	for name, signature := range variants {
		if Equal(base, signature) {
			t.Errorf("changing %s did not change the signature", name)
		}
	}
}

// With a "." separator these two requests would sign the same string.
func TestDotsCannotMoveBetweenFields(t *testing.T) {
	digest := BodyDigest(nil)
	first := IngressSignature("secret", 100, "tenant", "source", "evt.receipt", "forwarded", digest)
	second := IngressSignature("secret", 100, "tenant", "source", "evt", "receipt.forwarded", digest)
	if Equal(first, second) {
		t.Fatal("a dot moved between event ID and type without changing the signature")
	}
}

func TestParseFreshTimestamp(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	cases := []struct {
		name  string
		value string
		valid bool
	}{
		{"now", "1000000", true},
		{"at the past limit", strconv.Itoa(1_000_000 - ToleranceSeconds), true},
		{"past the past limit", strconv.Itoa(1_000_000 - ToleranceSeconds - 1), false},
		{"at the future limit", strconv.Itoa(1_000_000 + ToleranceSeconds), true},
		{"past the future limit", strconv.Itoa(1_000_000 + ToleranceSeconds + 1), false},
		{"not a number", "soon", false},
		{"empty", "", false},
	}
	for _, testCase := range cases {
		_, err := ParseFreshTimestamp(testCase.value, now)
		if (err == nil) != testCase.valid {
			t.Errorf("%s: got error %v, want valid=%v", testCase.name, err, testCase.valid)
		}
	}
}

func TestValidID(t *testing.T) {
	for _, value := range []string{"a", "evt-1", "receipt.forwarded", "a:b_c"} {
		if !ValidID(value) {
			t.Errorf("%q should be valid", value)
		}
	}
	for _, value := range []string{"", "has space", "new\nline", string(make([]byte, 201))} {
		if ValidID(value) {
			t.Errorf("%q should be invalid", value)
		}
	}
}
