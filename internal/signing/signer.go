package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Sign computes the HMAC-SHA256 signature for a single webhook delivery attempt.
// secret is the raw signing secret bytes for the endpoint.
// ts is the Unix epoch timestamp (whole seconds) of this specific attempt.
// body is the raw request body bytes.
// Returns the lowercase hexadecimal encoding of the HMAC-SHA256 digest.
func Sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
