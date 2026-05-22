package delivery

import (
	"net/http"
	"time"
)

// NewHTTPClient returns an *http.Client configured per plan.md:
// 30 s timeout, at most 1 redirect followed.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 1 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}
