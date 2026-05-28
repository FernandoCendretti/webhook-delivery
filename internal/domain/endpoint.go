package domain

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidURL is returned when an endpoint URL fails validation.
var ErrInvalidURL = errors.New("invalid endpoint url")

// Endpoint represents a registered webhook target URL.
type Endpoint struct {
	ID            uuid.UUID
	URL           string
	TenantID      uuid.UUID
	CreatedAt     time.Time
	SigningSecret []byte // non-nil only when returned by Insert or explicit secret fetch
}

// ValidateURL checks that rawURL is non-empty, within 2048 chars, and uses the
// http or https scheme with a non-empty host.
func ValidateURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("%w: empty", ErrInvalidURL)
	}
	if len(rawURL) > 2048 {
		return fmt.Errorf("%w: exceeds 2048 chars", ErrInvalidURL)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https, got %q", ErrInvalidURL, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: missing host", ErrInvalidURL)
	}
	return nil
}
