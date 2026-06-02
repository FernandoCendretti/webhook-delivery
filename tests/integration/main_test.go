//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"testing"
)

// TestMain starts a single shared Kafka container for the whole test suite.
// Each test gets an isolated topic (unique name) so no cross-test pollution.
// Postgres is NOT shared — each test calls setupAPI(t) to get a clean database.
func TestMain(m *testing.M) {
	addr, terminate, err := newKafkaContainer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: could not start Kafka container: %v\n", err)
		os.Exit(1)
	}
	sharedKafkaBroker = addr

	code := m.Run()

	terminate()
	os.Exit(code)
}
