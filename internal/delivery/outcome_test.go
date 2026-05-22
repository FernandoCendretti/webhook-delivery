package delivery_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int   // 0 = use err instead
		err        error // nil = use statusCode
		want       domain.AttemptOutcome
	}{
		{"200 OK", 200, nil, domain.OutcomeSuccess},
		{"204 No Content", 204, nil, domain.OutcomeSuccess},
		{"301 Redirect followed", 301, nil, domain.OutcomeSuccess},
		{"400 Bad Request", 400, nil, domain.OutcomePermanentFailure},
		{"404 Not Found", 404, nil, domain.OutcomePermanentFailure},
		{"429 Too Many Requests", 429, nil, domain.OutcomeTransientFailure},
		{"500 Internal Error", 500, nil, domain.OutcomeTransientFailure},
		{"503 Service Unavailable", 503, nil, domain.OutcomeTransientFailure},
		{"deadline exceeded", 0, context.DeadlineExceeded, domain.OutcomeTimeout},
		{"dial error", 0, errors.New("dial tcp: connection refused"), domain.OutcomeTransientFailure},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var resp *http.Response
			var err error

			if tc.err != nil {
				err = tc.err
			} else {
				rec := httptest.NewRecorder()
				rec.Code = tc.statusCode
				resp = rec.Result()
			}

			got := delivery.Classify(resp, err)
			if got != tc.want {
				t.Errorf("Classify(%d, %v) = %q, want %q", tc.statusCode, tc.err, got, tc.want)
			}
		})
	}
}
