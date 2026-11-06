package orchestrator

import "testing"

func TestCardHasher(t *testing.T) {
	if _, err := NewCardHasher([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
	a, _ := NewCardHasher(testKey)
	b, _ := NewCardHasher([]byte("another-key-another-key-another-key"))

	h1, err := a.Hash("4242424242424242")
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := a.Hash("4242424242424242")
	h3, _ := b.Hash("4242424242424242")
	h4, _ := a.Hash("4000056655665556")

	if h1 != h2 {
		t.Error("same card, same key: hashes differ")
	}
	if h1 == h3 {
		t.Error("hash does not depend on the key")
	}
	if h1 == h4 {
		t.Error("different cards share a hash")
	}
}
