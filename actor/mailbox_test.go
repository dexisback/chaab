package actor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

func TestMailbox_FIFODelivery(t *testing.T) {
	mb := actor.NewMailBox(10)
	ctx := context.Background()

	const count = 10
	for i := 0; i < count; i++ {
		if err := mb.Send(ctx, i); err != nil {
			t.Fatalf("send %d failed: %v", i, err)
		}
	}

	for i := 0; i < count; i++ {
		msg, err := mb.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %d failed: %v", i, err)
		}
		if msg != i {
			t.Fatalf("expected message %d, got %v", i, msg)
		}
	}
}

func TestMailbox_BoundedCapacityAndBlocking(t *testing.T) {
	mb := actor.NewMailBox(1)
	ctx := context.Background()

	if err := mb.Send(ctx, "first"); err != nil {
		t.Fatalf("send first failed: %v", err)
	}

	// Next send should block
	sendStarted := make(chan struct{})
	sendDone := make(chan error, 1)

	go func() {
		close(sendStarted)
		sendDone <- mb.Send(ctx, "second")
	}()

	<-sendStarted

	// Ensure the second send is blocked and hasn't finished
	select {
	case err := <-sendDone:
		t.Fatalf("expected send to block, finished with: %v", err)
	case <-time.After(20 * time.Millisecond):
		// Expected: still blocked
	}

	// Dequeue first message; second send should now unblock
	msg, err := mb.Receive(ctx)
	if err != nil || msg != "first" {
		t.Fatalf("receive first failed: msg=%v, err=%v", msg, err)
	}

	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("send second failed after unblocking: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second send to unblock")
	}

	// Verify second message was delivered
	msg, err = mb.Receive(ctx)
	if err != nil || msg != "second" {
		t.Fatalf("receive second failed: msg=%v, err=%v", msg, err)
	}
}

func TestMailbox_ContextCancellation_Send(t *testing.T) {
	mb := actor.NewMailBox(1)
	ctx := context.Background()

	if err := mb.Send(ctx, "fill"); err != nil {
		t.Fatalf("fill failed: %v", err)
	}

	// Already cancelled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mb.Send(canceledCtx, "overflow")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	// Context cancelled while blocked
	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelTimeout()

	err = mb.Send(ctxTimeout, "blocked")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}

func TestMailbox_ContextCancellation_Receive(t *testing.T) {
	mb := actor.NewMailBox(1)

	// Already cancelled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := mb.Receive(canceledCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	// Context cancelled while waiting on empty mailbox
	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelTimeout()

	_, err = mb.Receive(ctxTimeout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}

func TestMailbox_NilContextAndNilMessage(t *testing.T) {
	mb := actor.NewMailBox(1)

	if err := mb.Send(nil, "msg"); !errors.Is(err, actor.ErrNilContext) {
		t.Fatalf("expected ErrNilContext for nil ctx send, got: %v", err)
	}

	if _, err := mb.Receive(nil); !errors.Is(err, actor.ErrNilContext) {
		t.Fatalf("expected ErrNilContext for nil ctx receive, got: %v", err)
	}

	if err := mb.Send(context.Background(), nil); !errors.Is(err, actor.ErrInvalidMessage) {
		t.Fatalf("expected ErrInvalidMessage for nil message, got: %v", err)
	}
}

func TestMailbox_SafeConcurrentSendsAndReceives(t *testing.T) {
	mb := actor.NewMailBox(20)
	ctx := context.Background()

	const producers = 8
	const perProducer = 50
	const total = producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)

	for p := 0; p < producers; p++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := mb.Send(ctx, base+i); err != nil {
					t.Errorf("send failed: %v", err)
					return
				}
			}
		}(p * perProducer)
	}

	receivedCount := atomic.Int64{}
	var recvWg sync.WaitGroup
	recvWg.Add(4)

	for r := 0; r < 4; r++ {
		go func() {
			defer recvWg.Done()
			for {
				msg, err := mb.Receive(ctx)
				if err != nil {
					if errors.Is(err, actor.ErrActorStopped) {
						return
					}
					t.Errorf("receive error: %v", err)
					return
				}
				if msg != nil {
					receivedCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	mb.Close()
	recvWg.Wait()

	if receivedCount.Load() != int64(total) {
		t.Fatalf("expected %d messages, got %d", total, receivedCount.Load())
	}
}

func TestMailbox_GracefulClosure_DrainsAcceptedMessagesAndRejectsNew(t *testing.T) {
	mb := actor.NewMailBox(5)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if err := mb.Send(ctx, i); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	mb.Close()

	// New sends must be rejected after closure
	if err := mb.Send(ctx, 4); !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped for send after close, got: %v", err)
	}

	// Accepted messages must be drained
	for i := 1; i <= 3; i++ {
		msg, err := mb.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %d failed: %v", i, err)
		}
		if msg != i {
			t.Fatalf("expected %d, got %v", i, msg)
		}
	}

	// Once drained, Receive must report ErrActorStopped
	_, err := mb.Receive(ctx)
	if !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped after draining, got: %v", err)
	}

	// Subsequent Receive calls continue reporting ErrActorStopped
	_, err = mb.Receive(ctx)
	if !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped on repeat receive, got: %v", err)
	}
}

func TestMailbox_ConcurrentCloseSafeAndIdempotent(t *testing.T) {
	mb := actor.NewMailBox(5)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mb.Close()
		}()
	}

	wg.Wait()

	// Subsequent sends must return ErrActorStopped
	if err := mb.Send(context.Background(), "test"); !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped, got: %v", err)
	}
}

func TestMailbox_BlockedSendersUnblockedOnClose(t *testing.T) {
	mb := actor.NewMailBox(1)
	ctx := context.Background()

	if err := mb.Send(ctx, "fill"); err != nil {
		t.Fatalf("fill failed: %v", err)
	}

	var wg sync.WaitGroup
	const senders = 5
	errs := make(chan error, senders)

	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			errs <- mb.Send(ctx, val)
		}(i)
	}

	// Give senders time to block
	time.Sleep(10 * time.Millisecond)

	mb.Close()
	wg.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, actor.ErrActorStopped) {
			t.Fatalf("expected blocked senders to return ErrActorStopped, got: %v", err)
		}
	}
}
