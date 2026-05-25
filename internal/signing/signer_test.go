package signing_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/FernandoCendretti/webhook-delivery/internal/signing"
)

// TestSign_KnownVector computes the expected digest independently using the Go
// stdlib and asserts that Sign returns the same value for the same inputs.
func TestSign_KnownVector(t *testing.T) {
	secret := []byte("test-secret")
	ts := int64(1700000000)
	body := []byte(`{"foo":"bar"}`)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))

	got := signing.Sign(secret, ts, body)
	if got != want {
		t.Errorf("Sign() = %q, want %q", got, want)
	}
}

// TestSign_EmptyBody asserts that Sign still produces a valid 64-char lowercase
// hex digest when the body is empty.
func TestSign_EmptyBody(t *testing.T) {
	sig := signing.Sign([]byte("any-secret"), 1000000000, []byte{})
	if len(sig) != 64 {
		t.Errorf("len(sig) = %d, want 64", len(sig))
	}
	if strings.ToLower(sig) != sig {
		t.Errorf("sig is not lowercase: %q", sig)
	}
}

// TestSign_OutputInvariants asserts that Sign always returns a 64-char
// lowercase hex string regardless of input.
func TestSign_OutputInvariants(t *testing.T) {
	cases := []struct {
		name   string
		secret []byte
		ts     int64
		body   []byte
	}{
		{"nil body", []byte("secret"), 0, nil},
		{"large ts", []byte("secret"), 9223372036854775807, []byte("large timestamp")},
		{"binary secret", []byte{0x00, 0xFF, 0xAB}, 123456789, []byte("body")},
		{"zero ts", []byte("k"), 0, []byte("payload")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := signing.Sign(tc.secret, tc.ts, tc.body)
			if len(sig) != 64 {
				t.Errorf("len(sig) = %d, want 64", len(sig))
			}
			if strings.ToLower(sig) != sig {
				t.Errorf("sig is not lowercase: %q", sig)
			}
		})
	}
}
