package banking

import (
	"context"
	"time"

	"github.com/dexisback/chaab/actor"
)

type SimulationReport struct {
	AmaanBalance     int64
	AyaanBalance     int64
	TotalProcessed   uint64
	TotalRestarts    uint64
	TotalDeadLetters uint64
}

func RunSimulation(ctx context.Context, dispatcher actor.Dispatcher) (*SimulationReport, error) {
	bank := NewBank(dispatcher)
	defer bank.Shutdown(context.Background()) // ensure cleanup on return

	supervisor := actor.NewOneForOneStrategy(3, time.Minute, nil) // allow up to 3 restarts

	alice, err := bank.OpenAccount("alice", 1000, 10, actor.OverflowBlock, supervisor)
	if err != nil {
		return nil, err
	}

	bob, err := bank.OpenAccount("bob", 500, 10, actor.OverflowBlock, supervisor)
	if err != nil {
		return nil, err
	}

	charlie, err := bank.OpenAccount("charlie", 100, 1, actor.OverflowFailFast, nil)
	if err != nil {
		return nil, err
	}

	reply := make(chan error, 1)
	if err := alice.Tell(ctx, Transfer{To: bob.ID(), Amount: 200, ReplyTo: reply}); err != nil {
		return nil, err
	}
	if err := <-reply; err != nil {
		return nil, err // transfer failed
	}

	_ = alice.MailboxDepth() // inspect queue depth

	_ = bob.Tell(ctx, Crash{Reason: "simulated unhandled panic"}) // crash bob
	_ = bob.Tell(ctx, Deposit{Amount: 50})                        // processed after restart

	_ = charlie.Tell(ctx, Deposit{Amount: 10})
	_ = charlie.Tell(ctx, Deposit{Amount: 20}) // triggers overflow fail-fast and dead letter

	_ = bank.System().Stop(charlie.ID())       // stop charlie
	_ = charlie.Tell(ctx, Deposit{Amount: 30}) // routed to dead letters

	time.Sleep(100 * time.Millisecond) // wait for message drainage

	amaanBal, err := bank.QueryBalance(ctx, alice.ID())
	if err != nil {
		return nil, err
	}

	ayaanBal, err := bank.QueryBalance(ctx, bob.ID())
	if err != nil {
		return nil, err
	}

	stats := bank.System().Stats()
	return &SimulationReport{
		AmaanBalance:     amaanBal,
		AyaanBalance:     ayaanBal,
		TotalProcessed:   stats.TotalProcessed,
		TotalRestarts:    stats.TotalRestarts,
		TotalDeadLetters: stats.TotalDeadLetters,
	}, nil
}
