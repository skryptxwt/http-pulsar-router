package pulsarpub

import (
	"context"
	"errors"
	"testing"
)

func TestPublisherRejectsPublishAfterClose(t *testing.T) {
	p := &Publisher{
		producers: make(map[string]*producerSlot),
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	err := p.Publish(context.Background(), "topic", "key", []byte("payload"), nil)
	if !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("publish after close error = %v", err)
	}
}
