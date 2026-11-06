//this file defines the scheduling contract
//dispatcher manages actor execution, while scheduler decides which actors get execution time and when 

//NOTE: our initial version only starts one goroutine per actor. eventually, we'll introduce scheudlers that distribute actor work across shared workers



package actor


import "context"

//type represents a unit of work submitted for execution
type Task func()


type Scheduler interface {
	Schedule(ctx context.Context, task Task) error 	


	
	Shutdown(ctx context.Context) error 	// Shutdown stops the scheduler and its workers.

}


//task -> task that needs to execute
//schedule -> submits work according to the scheduler's strategy
//NOTE: later on, 
// 	:goroutineScheduler = starts a goroutine per task,
// 	:workStealingScheduler = distributes tasks among shared workers,
// workerpoolScheduler = submits tasks to a shared worker pool 

