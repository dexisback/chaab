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

func TestMailbox_OverflowFailFast(t *testing.T) {
	mb := actor.NewMailboxWithStrategy(2, actor.OverflowFailFast)
	ctx := context.Background()

	if err := mb.Send(ctx, "msg1"); err != nil {
		t.Fatalf("send 1 failed: %v", err)
	}
	if err := mb.Send(ctx, "msg2"); err != nil {
		t.Fatalf("send 2 failed: %v", err)
	}

	// 3rd message exceeds capacity 2: must immediately return ErrMailboxFull
	err := mb.Send(ctx, "msg3")
	if !errors.Is(err, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull, got: %v", err)
	}
}

func TestMailbox_OverflowDropOldest(t *testing.T) {
	mb := actor.NewMailboxWithStrategy(3, actor.OverflowDropOldest)
	ctx := context.Background()

	// Fill to capacity
	_ = mb.Send(ctx, 1)
	_ = mb.Send(ctx, 2)
	_ = mb.Send(ctx, 3)

	// Send 4 and 5 -> should evict 1 and 2
	if err := mb.Send(ctx, 4); err != nil {
		t.Fatalf("send 4 failed: %v", err)
	}
	if err := mb.Send(ctx, 5); err != nil {
		t.Fatalf("send 5 failed: %v", err)
	}

	// Drain messages: expected [3, 4, 5]
	expected := []int{3, 4, 5}
	for i, exp := range expected {
		msg, err := mb.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %d failed: %v", i, err)
		}
		if msg != exp {
			t.Fatalf("at index %d: expected %d, got %v", i, exp, msg)
		}
	}
}

func TestMailbox_OverflowDropNewest(t *testing.T) {
	mb := actor.NewMailboxWithStrategy(3, actor.OverflowDropNewest)
	ctx := context.Background()

	// Fill to capacity
	_ = mb.Send(ctx, 1)
	_ = mb.Send(ctx, 2)
	_ = mb.Send(ctx, 3)

	// Send 4 and 5 -> should be discarded without error
	if err := mb.Send(ctx, 4); err != nil {
		t.Fatalf("send 4 failed: %v", err)
	}
	if err := mb.Send(ctx, 5); err != nil {
		t.Fatalf("send 5 failed: %v", err)
	}

	// Drain messages: expected [1, 2, 3]
	expected := []int{1, 2, 3}
	for i, exp := range expected {
		msg, err := mb.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %d failed: %v", i, err)
		}
		if msg != exp {
			t.Fatalf("at index %d: expected %d, got %v", i, exp, msg)
		}
	}
}

func TestSystem_CustomMailboxCapacityAndStrategy(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	// Create an actor with capacity 1 and OverflowFailFast
	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if msg == "block" {
				close(started)
				<-release
			}
		},
	},
		actor.WithMailboxCapacity(1),
		actor.WithOverflowStrategy(actor.OverflowFailFast),
	)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	if ref.Mailbox().Capacity() != 1 {
		t.Fatalf("expected capacity 1, got %d", ref.Mailbox().Capacity())
	}
	if ref.Mailbox().Strategy() != actor.OverflowFailFast {
		t.Fatalf("expected OverflowFailFast, got %v", ref.Mailbox().Strategy())
	}

	// Active message occupies the worker
	if err := ref.Tell(context.Background(), "block"); err != nil {
		t.Fatalf("block tell failed: %v", err)
	}
	<-started

	// Fill the single buffer slot
	if err := ref.Tell(context.Background(), "first"); err != nil {
		t.Fatalf("first tell failed: %v", err)
	}

	// Second buffered tell should immediately fail fast with ErrMailboxFull
	err = ref.Tell(context.Background(), "second")
	if !errors.Is(err, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull on second tell, got: %v", err)
	}
}

func TestMailbox_ConcurrentOverflowDropOldest(t *testing.T) {
	const capacity = 5
	mb := actor.NewMailboxWithStrategy(capacity, actor.OverflowDropOldest)
	ctx := context.Background()

	var wg sync.WaitGroup
	const senders = 10
	const msgsPerSender = 50

	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(sID int) {
			defer wg.Done()
			for i := 0; i < msgsPerSender; i++ {
				_ = mb.Send(ctx, sID*1000+i)
			}
		}(s)
	}

	wg.Wait()

	// Drain remaining messages: capacity must be bounded at exactly 5
	drained := 0
	for {
		_, ok, _ := mb.TryReceive()
		if !ok {
			break
		}
		drained++
	}

	if drained != capacity {
		t.Fatalf("expected mailbox to contain exactly %d items, got %d", capacity, drained)
	}
}
