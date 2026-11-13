package benchmarks

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dexisback/chaab/actor"
)

type countActor struct {
	remaining atomic.Int64
	done      chan struct{}
}

func (a *countActor) Receive(ctx context.Context, message any) error {
	if a.remaining.Add(-1) == 0 {
		close(a.done) // signal completion when all expected messages arrive
	}
	return nil
}

type pingMsg struct {
	sender *actor.ActorRef
	sentAt time.Time
}

type pongMsg struct {
	sentAt time.Time
}

type pingActor struct {
	recorder *LatencyRecorder
	target   *actor.ActorRef
	selfRef  *actor.ActorRef
	rounds   int
	current  int
	done     chan struct{}
}

func (a *pingActor) Receive(ctx context.Context, message any) error {
	switch msg := message.(type) {
	case pongMsg:
		if a.recorder != nil {
			a.recorder.Record(time.Since(msg.sentAt)) // record round-trip latency
		}
		a.current++
		if a.current >= a.rounds {
			close(a.done) // ping-pong cycles completed
			return nil
		}
		return a.target.Tell(ctx, pingMsg{sender: a.selfRef, sentAt: time.Now()}) // send next ping
	default:
		return nil
	}
}

type pongActor struct{}

func (a *pongActor) Receive(ctx context.Context, message any) error {
	switch msg := message.(type) {
	case pingMsg:
		if msg.sender != nil {
			return msg.sender.Tell(ctx, pongMsg{sentAt: msg.sentAt}) // reply with pong
		}
	}
	return nil
}

type heavyComputeActor struct {
	done chan struct{}
	ops  int
}

func (a *heavyComputeActor) Receive(ctx context.Context, message any) error {
	var acc uint64 = 1
	for i := 1; i <= a.ops; i++ {
		acc = (acc * uint64(i)) ^ 0x5bf03635 // simulate CPU work
	}
	_ = acc
	if a.done != nil {
		select {
		case a.done <- struct{}{}:
		default:
		}
	}
	return nil
}
