package benchmarks

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

func newDispatcher(name string, workers int) actor.Dispatcher {
	switch name {
	case "Goroutine":
		return actor.NewGoroutineDispatcher()
	case "WorkerPool":
		return actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{Workers: workers, MessageBudget: 100})
	case "WorkStealing":
		return actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{MaxWorkers: workers, MaxMessageBudget: 100})
	default:
		panic("unknown dispatcher: " + name)
	}
}

func BenchmarkThroughput(b *testing.B) {
	dispatchers := []string{"Goroutine", "WorkerPool", "WorkStealing"}
	numWorkers := runtime.NumCPU()

	for _, name := range dispatchers {
		b.Run(name, func(b *testing.B) {
			d := newDispatcher(name, numWorkers)
			sys := actor.NewSystem(actor.WithDispatcher(d))
			defer sys.Shutdown(context.Background())

			done := make(chan struct{})
			consumer := &countActor{done: done}
			consumer.remaining.Store(int64(b.N))

			ref, err := sys.CreateActor(consumer, actor.WithMailboxCapacity(b.N+1000))
			if err != nil {
				b.Fatalf("create actor failed: %v", err)
			}

			ctx := context.Background()
			b.ReportAllocs() // track heap allocations
			b.ResetTimer()   // exclude setup from timing

			for i := 0; i < b.N; i++ {
				_ = ref.Tell(ctx, i)
			}
			<-done // wait for actor to finish draining all messages
			b.StopTimer()
		})
	}
}

func BenchmarkFanOut(b *testing.B) {
	dispatchers := []string{"Goroutine", "WorkerPool", "WorkStealing"}
	numWorkers := runtime.NumCPU()
	const actorCount = 5000

	for _, name := range dispatchers {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for iter := 0; iter < b.N; iter++ {
				b.StopTimer() // pause during actor creation
				d := newDispatcher(name, numWorkers)
				sys := actor.NewSystem(actor.WithDispatcher(d))

				var wg sync.WaitGroup
				wg.Add(actorCount)

				refs := make([]*actor.ActorRef, actorCount)
				for i := 0; i < actorCount; i++ {
					done := make(chan struct{})
					act := &countActor{done: done}
					act.remaining.Store(1)
					r, _ := sys.CreateActor(act, actor.WithMailboxCapacity(2))
					refs[i] = r

					go func(c chan struct{}) {
						<-c
						wg.Done()
					}(done)
				}

				ctx := context.Background()
				b.StartTimer() // measure broadcast message delivery

				for i := 0; i < actorCount; i++ {
					_ = refs[i].Tell(ctx, i)
				}
				wg.Wait()
				b.StopTimer()

				_ = sys.Shutdown(context.Background())
			}
		})
	}
}

func BenchmarkPingPong(b *testing.B) {
	dispatchers := []string{"Goroutine", "WorkerPool", "WorkStealing"}
	numWorkers := runtime.NumCPU()

	for _, name := range dispatchers {
		b.Run(name, func(b *testing.B) {
			d := newDispatcher(name, numWorkers)
			sys := actor.NewSystem(actor.WithDispatcher(d))
			defer sys.Shutdown(context.Background())

			pongRef, _ := sys.CreateActor(&pongActor{}, actor.WithMailboxCapacity(100))

			done := make(chan struct{})
			recorder := NewLatencyRecorder(b.N)
			pinger := &pingActor{
				recorder: recorder,
				target:   pongRef,
				rounds:   b.N,
				done:     done,
			}
			pingRef, _ := sys.CreateActor(pinger, actor.WithMailboxCapacity(100))
			pinger.selfRef = pingRef

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()

			_ = pongRef.Tell(ctx, pingMsg{sender: pingRef, sentAt: time.Now()})
			<-done // wait for ping pong cycles to complete
			b.StopTimer()

			summary := recorder.Summary()
			b.Logf("[%s] p50: %v, p95: %v, p99: %v", name, summary.P50, summary.P95, summary.P99)
		})
	}
}

func BenchmarkWorkStealingSkewed(b *testing.B) {
	dispatchers := []string{"WorkerPool", "WorkStealing"}
	numWorkers := 4
	const totalTasks = 200
	const workOps = 20000

	for _, name := range dispatchers {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				d := newDispatcher(name, numWorkers)
				sys := actor.NewSystem(actor.WithDispatcher(d))

				done := make(chan struct{}, totalTasks)
				hotActor, _ := sys.CreateActor(&heavyComputeActor{done: done, ops: workOps}, actor.WithMailboxCapacity(totalTasks+10))

				ctx := context.Background()
				b.StartTimer() // measure time to process skewed workload

				for i := 0; i < totalTasks; i++ {
					_ = hotActor.Tell(ctx, i)
				}

				for i := 0; i < totalTasks; i++ {
					<-done
				}
				b.StopTimer()

				_ = sys.Shutdown(context.Background())
			}
		})
	}
}
