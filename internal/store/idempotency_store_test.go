package store

import (
	"testing"

	"github.com/google/uuid"
)

func TestLockKey_Determinism(t *testing.T) {
	epID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	key := "test-key-abc"

	first := lockKey(epID, key)
	second := lockKey(epID, key)
	if first != second {
		t.Errorf("lockKey not deterministic: %d != %d", first, second)
	}
}

func TestLockKey_DifferentKeys_DifferentValues(t *testing.T) {
	epID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	a := lockKey(epID, "key-alpha")
	b := lockKey(epID, "key-beta")
	if a == b {
		t.Errorf("different keys produced same lock key: %d", a)
	}
}

func TestLockKey_DifferentEndpoints_DifferentValues(t *testing.T) {
	ep1 := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	ep2 := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	key := "same-key"

	a := lockKey(ep1, key)
	b := lockKey(ep2, key)
	if a == b {
		t.Errorf("different endpoints produced same lock key: %d", a)
	}
}
