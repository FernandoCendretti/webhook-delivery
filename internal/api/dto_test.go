package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestEndpointResponse_NoSigningSecret enforces that marshalling EndpointResponse
// (used by read operations) never includes the "signing_secret" key in the JSON
// output — regardless of struct field additions or changes (SC-005, FR-002).
func TestEndpointResponse_NoSigningSecret(t *testing.T) {
	resp := EndpointResponse{}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("json.Marshal EndpointResponse: %v", err)
	}
	if strings.Contains(string(b), "signing_secret") {
		t.Errorf("EndpointResponse JSON contains signing_secret: %s", b)
	}
}
