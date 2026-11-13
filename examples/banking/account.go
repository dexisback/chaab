package banking

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/dexisback/chaab/actor"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAmount     = errors.New("invalid transaction amount")
)

type AccountActor struct {
	id             string
	balance        int64
	initialBalance int64
	system         *actor.System
	preStarts      atomic.Int32
	postStops      atomic.Int32
}

func NewAccountActor(id string, initialBalance int64, sys *actor.System) *AccountActor {
	return &AccountActor{
		id:             id,
		balance:        initialBalance,
		initialBalance: initialBalance,
		system:         sys,
	}
}

func (a *AccountActor) PreStart(ctx context.Context) error {
	a.preStarts.Add(1) // count lifecycle hook invocations
	return nil
}

func (a *AccountActor) PostStop(ctx context.Context) error {
	a.postStops.Add(1) // cleanup resources on stop
	return nil
}

func (a *AccountActor) Receive(ctx context.Context, message any) error {
	switch msg := message.(type) {
	case Deposit:
		if msg.Amount <= 0 {
			return ErrInvalidAmount // reject invalid deposits
		}
		a.balance += msg.Amount // credit account balance
		return nil

	case Withdraw:
		if msg.Amount <= 0 {
			if msg.ReplyTo != nil {
				msg.ReplyTo <- ErrInvalidAmount
			}
			return ErrInvalidAmount
		}
		if a.balance < msg.Amount {
			if msg.ReplyTo != nil {
				msg.ReplyTo <- ErrInsufficientFunds
			}
			return ErrInsufficientFunds
		}
		a.balance -= msg.Amount // debit account balance
		if msg.ReplyTo != nil {
			msg.ReplyTo <- nil
		}
		return nil

	case Transfer:
		if msg.Amount <= 0 {
			if msg.ReplyTo != nil {
				msg.ReplyTo <- ErrInvalidAmount
			}
			return ErrInvalidAmount
		}
		if a.balance < msg.Amount {
			if msg.ReplyTo != nil {
				msg.ReplyTo <- ErrInsufficientFunds
			}
			return ErrInsufficientFunds
		}
		targetRef, exists := a.system.GetActor(msg.To)
		if !exists {
			err := fmt.Errorf("recipient account %s not found", msg.To)
			if msg.ReplyTo != nil {
				msg.ReplyTo <- err
			}
			return err
		}
		a.balance -= msg.Amount                                 // debit sender
		err := targetRef.Tell(ctx, Deposit{Amount: msg.Amount}) // credit recipient asynchronously
		if msg.ReplyTo != nil {
			msg.ReplyTo <- err
		}
		return err

	case GetBalance:
		if msg.ReplyTo != nil {
			msg.ReplyTo <- a.balance // return current balance
		}
		return nil

	case Crash:
		panic("simulated account crash: " + msg.Reason) // trigger supervisor recovery

	default:
		return fmt.Errorf("unhandled message type: %T", message)
	}
}
