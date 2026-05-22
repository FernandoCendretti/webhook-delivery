package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateURL(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid http", input: "http://example.com/webhook", wantErr: false},
		{name: "valid https", input: "https://example.com/webhook", wantErr: false},
		{name: "valid https with port and query", input: "https://example.com:8443/hook?x=1", wantErr: false},
		{name: "scheme case-insensitive", input: "HTTPS://Example.com/webhook", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "malformed", input: "://nope", wantErr: true},
		{name: "ftp scheme rejected", input: "ftp://example.com/file", wantErr: true},
		{name: "missing scheme", input: "example.com/webhook", wantErr: true},
		{name: "missing host", input: "https:///path", wantErr: true},
		{name: "exceeds 2048 chars", input: "https://example.com/" + strings.Repeat("a", 2048), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateURL(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateURL(%q): want error, got nil", tc.input)
				}
				if !errors.Is(err, ErrInvalidURL) {
					t.Errorf("ValidateURL(%q): error %v does not wrap ErrInvalidURL", tc.input, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateURL(%q): unexpected error %v", tc.input, err)
			}
		})
	}
}
