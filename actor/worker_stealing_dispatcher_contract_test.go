package actor_test

import (
	"testing"

	"github.com/dexisback/chaab/actor"
)

// TestWorkStealingDispatcher_Contract runs the standard Dispatcher
// conformance suite against WorkStealingDispatcher.
func TestWorkStealingDispatcher_Contract(t *testing.T) {
	RunDispatcherContractTests(t, func() actor.Dispatcher {
		return actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{
			MaxWorkers:       4,
			MaxMessageBudget: 10,
		})
	})
}
