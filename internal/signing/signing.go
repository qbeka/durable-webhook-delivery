// Package signing implements the HMAC signatures from docs/contract.md.
//
// Fields are joined with newlines. HTTP header values cannot contain a newline,
// so two different requests can never produce the same signed string.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ToleranceSeconds is how far a signed timestamp may be from the server clock.
const ToleranceSeconds = 300

var idPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,200}$`)

// ValidID reports whether value can be used as a tenant, source, event ID or type.
func ValidID(value string) bool {
	return idPattern.MatchString(value)
}

// BodyDigest returns the raw SHA-256 of a request body.
func BodyDigest(body []byte) []byte {
	sum := sha256.Sum256(body)
	return sum[:]
}

// IngressSignature signs a request from a source to the ingress endpoint.
func IngressSignature(secret string, unixSeconds int64, tenant, source, eventID, eventType string, bodyDigest []byte) string {
	return sign(secret, strconv.FormatInt(unixSeconds, 10), tenant, source, eventID, eventType, hex.EncodeToString(bodyDigest))
}

// DeliverySignature signs a request from a worker to a destination.
func DeliverySignature(secret string, unixSeconds int64, deliveryKey string, bodyDigest []byte) string {
	return sign(secret, strconv.FormatInt(unixSeconds, 10), deliveryKey, hex.EncodeToString(bodyDigest))
}

func sign(secret string, fields ...string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strings.Join(fields, "\n")))
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Equal compares two signatures in constant time.
func Equal(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}

// ErrStaleTimestamp means the timestamp is unreadable or outside the tolerance.
var ErrStaleTimestamp = errors.New("timestamp outside tolerance")

// ParseFreshTimestamp reads a Unix-seconds header value and checks the tolerance.
func ParseFreshTimestamp(value string, now time.Time) (int64, error) {
	unixSeconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, ErrStaleTimestamp
	}
	skewSeconds := now.Unix() - unixSeconds
	if skewSeconds > ToleranceSeconds || skewSeconds < -ToleranceSeconds {
		return 0, ErrStaleTimestamp
	}
	return unixSeconds, nil
}
