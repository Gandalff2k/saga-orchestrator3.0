package config

import (
	"testing"
	"time"
)

func TestLoad_ConsumerWithNestedRetryPolicy(t *testing.T) {
	t.Setenv("CONSUMER_GROUP", "inventory")
	t.Setenv("CONSUMER_TOPICS", "inventory.commands,other")
	t.Setenv("CONSUMER_RETRY_MAX_ATTEMPTS", "7")
	t.Setenv("CONSUMER_RETRY_INITIAL_BACKOFF", "50ms")

	c, err := Load[Consumer]()
	if err != nil {
		t.Fatal(err)
	}
	if c.Group != "inventory" || len(c.Topics) != 2 {
		t.Errorf("group/topics = %q/%v", c.Group, c.Topics)
	}
	if c.Retry.MaxAttempts != 7 || c.Retry.Initial != 50*time.Millisecond {
		t.Errorf("retry overrides not applied: %+v", c.Retry)
	}
	if c.Retry.Max != 10*time.Second || c.Retry.Multiplier != 2 {
		t.Errorf("retry defaults not applied: %+v", c.Retry)
	}
	if c.MaxPollRecords != 500 || c.Concurrency != 8 {
		t.Errorf("defaults not applied: %+v", c)
	}
}

func TestLoad_RequiredMissing(t *testing.T) {
	t.Setenv("CONSUMER_TOPICS", "x")
	if _, err := Load[Consumer](); err == nil {
		t.Fatal("expected error for missing CONSUMER_GROUP")
	}
}
