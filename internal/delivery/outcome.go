package delivery

import (
	"context"
	"errors"
	"net/http"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// Classify maps an HTTP response (or transport error) to an AttemptOutcome.
//
// Classification matrix (per plan.md):
//   - nil error, 2xx             → success
//   - nil error, 3xx             → success  (redirect was followed up to the limit)
//   - nil error, 4xx (not 429)   → permanent_failure
//   - nil error, 429             → transient_failure (rate limited, retry)
//   - nil error, 5xx             → transient_failure
//   - context.DeadlineExceeded   → timeout
//   - any other transport error  → transient_failure
func Classify(resp *http.Response, err error) domain.AttemptOutcome {
	if err != nil {
		if isDeadline(err) {
			return domain.OutcomeTimeout
		}
		return domain.OutcomeTransientFailure
	}

	code := resp.StatusCode
	switch {
	case code >= 200 && code < 400:
		return domain.OutcomeSuccess
	case code == 429:
		return domain.OutcomeTransientFailure
	case code >= 400 && code < 500:
		return domain.OutcomePermanentFailure
	default: // 5xx
		return domain.OutcomeTransientFailure
	}
}

func isDeadline(err error) bool {
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
