package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const maxRetries = 3

// Task represents a unit of work.
type Task struct {
	id     int
	data   string
	retries int
}

// Worker function that processes tasks. If a worker fails, the task will be sent to failChan.
func worker(id int, taskChan <-chan Task, wg *sync.WaitGroup, failChan chan<- Task) {
	defer wg.Done()
	for task := range taskChan {
		fmt.Printf("Worker %d started task %d: %s (retry #%d)\n", id, task.id, task.data, task.retries)
		// Simulate random failure (30% chance of failure)
		if rand.Float32() < 0.3 {
			fmt.Printf("Worker %d failed on task %d\n", id, task.id)
			failChan <- task // Send the failed task for reassignment
			continue
		}
		// Simulate task processing time
		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
		fmt.Printf("Worker %d completed task %d\n", id, task.id)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	// Define a set of tasks to be executed
	tasks := []Task{
		{id: 1, data: "Task 1"},
		{id: 2, data: "Task 2"},
		{id: 3, data: "Task 3"},
		{id: 4, data: "Task 4"},
		{id: 5, data: "Task 5"},
	}

	// Channels for task distribution and failure handling
	taskChan := make(chan Task, len(tasks))
	failChan := make(chan Task, len(tasks))

	// WaitGroup to ensure all workers finish their tasks
	var wg sync.WaitGroup

	// Number of workers (simulating processors)
	numWorkers := 3

	// Start worker goroutines
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, taskChan, &wg, failChan)
	}

	// Assign tasks in a round-robin fashion
	for i, task := range tasks {
		fmt.Printf("Assigned task %d to worker %d\n", task.id, (i%numWorkers)+1)
		taskChan <- task
	}
	close(taskChan) // Close the task channel after all tasks are sent

	// Retry failed tasks
	go func() {
		for failedTask := range failChan {
			if failedTask.retries < maxRetries {
				failedTask.retries++
				fmt.Printf("Reassigning failed task %d (retry #%d)\n", failedTask.id, failedTask.retries)
				wg.Add(1)
				go worker(rand.Intn(numWorkers)+1, taskChan, &wg, failChan)
			} else {
				fmt.Printf("Task %d has failed after %d retries and will not be retried further.\n", failedTask.id, failedTask.retries)
			}
		}
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Close failChan to stop the goroutine
	close(failChan)

	fmt.Println("All tasks completed.")
}
