package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dexisback/chaab/actor"
	"github.com/dexisback/chaab/examples/banking"
)

func main() {
	ctx := context.Background()
	dispatcher := actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{Workers: 4}) // 4 worker threads

	report, err := banking.RunSimulation(ctx, dispatcher)
	if err != nil {
		log.Fatalf("simulation failed: %v", err)
	}

	fmt.Println("--- Bank Simulation Completed Successfully ---")
	fmt.Printf("Amaan Final Balance: $%d (Expected: $800)\n", report.AmaanBalance)
	fmt.Printf("Ayaan Final Balance:   $%d (Expected: $550 after restart)\n", report.AyaanBalance)
	fmt.Printf("Total Messages:      %d\n", report.TotalProcessed)
	fmt.Printf("Total Restarts:      %d\n", report.TotalRestarts)
	fmt.Printf("Dead Letters:        %d\n", report.TotalDeadLetters)
}
