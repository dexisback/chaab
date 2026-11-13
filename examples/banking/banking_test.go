package banking_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
	"github.com/dexisback/chaab/examples/banking"
)

func TestBanking_DepositAndWithdraw(t *testing.T) {
	bank := banking.NewBank(nil)
	defer bank.Shutdown(context.Background())

	ref, err := bank.OpenAccount("acc-1", 100, 10, actor.OverflowBlock, nil)
	if err != nil {
		t.Fatalf("open account failed: %v", err)
	}

	ctx := context.Background()
	if err := ref.Tell(ctx, banking.Deposit{Amount: 50}); err != nil {
		t.Fatalf("deposit failed: %v", err)
	}

	bal, err := bank.QueryBalance(ctx, "acc-1")
	if err != nil {
		t.Fatalf("query balance failed: %v", err)
	}
	if bal != 150 {
		t.Fatalf("expected balance 150, got %d", bal)
	}

	reply := make(chan error, 1)
	if err := ref.Tell(ctx, banking.Withdraw{Amount: 80, ReplyTo: reply}); err != nil {
		t.Fatalf("withdraw failed: %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("expected successful withdraw, got: %v", err)
	}

	bal, err = bank.QueryBalance(ctx, "acc-1")
	if err != nil {
		t.Fatalf("query balance failed: %v", err)
	}
	if bal != 70 {
		t.Fatalf("expected balance 70, got %d", bal)
	}

	if err := ref.Tell(ctx, banking.Withdraw{Amount: 1000, ReplyTo: reply}); err != nil {
		t.Fatalf("withdraw tell failed: %v", err)
	}
	if err := <-reply; !errors.Is(err, banking.ErrInsufficientFunds) {
		t.Fatalf("expected ErrInsufficientFunds, got: %v", err)
	}
}

func TestBanking_Transfer(t *testing.T) {
	bank := banking.NewBank(nil)
	defer bank.Shutdown(context.Background())

	alice, err := bank.OpenAccount("alice", 500, 10, actor.OverflowBlock, nil)
	if err != nil {
		t.Fatalf("open alice failed: %v", err)
	}
	bob, err := bank.OpenAccount("bob", 200, 10, actor.OverflowBlock, nil)
	if err != nil {
		t.Fatalf("open bob failed: %v", err)
	}

	ctx := context.Background()
	reply := make(chan error, 1)
	if err := alice.Tell(ctx, banking.Transfer{To: bob.ID(), Amount: 150, ReplyTo: reply}); err != nil {
		t.Fatalf("transfer tell failed: %v", err)
	}
	if err := <-reply; err != nil {
		t.Fatalf("transfer failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // wait for asynchronous credit to bob

	aliceBal, _ := bank.QueryBalance(ctx, alice.ID())
	bobBal, _ := bank.QueryBalance(ctx, bob.ID())

	if aliceBal != 350 {
		t.Fatalf("expected alice balance 350, got %d", aliceBal)
	}
	if bobBal != 350 {
		t.Fatalf("expected bob balance 350, got %d", bobBal)
	}
}

func TestBanking_SupervisedCrash(t *testing.T) {
	bank := banking.NewBank(nil)
	defer bank.Shutdown(context.Background())

	sup := actor.NewOneForOneStrategy(3, time.Minute, nil)
	bob, err := bank.OpenAccount("bob", 500, 10, actor.OverflowBlock, sup)
	if err != nil {
		t.Fatalf("open bob failed: %v", err)
	}

	ctx := context.Background()
	_ = bob.Tell(ctx, banking.Crash{Reason: "fatal crash"}) // triggers panic and supervisor restart
	_ = bob.Tell(ctx, banking.Deposit{Amount: 50})          // processed after fresh instance created

	time.Sleep(50 * time.Millisecond)

	bal, err := bank.QueryBalance(ctx, bob.ID())
	if err != nil {
		t.Fatalf("query balance failed: %v", err)
	}
	if bal != 550 {
		t.Fatalf("expected bob balance 550, got %d", bal)
	}

	stats := bank.System().Stats()
	if stats.TotalRestarts != 1 {
		t.Fatalf("expected 1 restart, got %d", stats.TotalRestarts)
	}
}

func TestBanking_RunSimulation(t *testing.T) {
	ctx := context.Background()
	report, err := banking.RunSimulation(ctx, nil)
	if err != nil {
		t.Fatalf("simulation failed: %v", err)
	}

	if report.AmaanBalance != 800 {
		t.Fatalf("expected alice balance 800, got %d", report.AmaanBalance)
	}
	if report.AyaanBalance != 550 {
		t.Fatalf("expected bob balance 550, got %d", report.AyaanBalance)
	}
	if report.TotalRestarts != 1 {
		t.Fatalf("expected 1 restart, got %d", report.TotalRestarts)
	}
	if report.TotalDeadLetters == 0 {
		t.Fatalf("expected >0 dead letters, got %d", report.TotalDeadLetters)
	}
}
