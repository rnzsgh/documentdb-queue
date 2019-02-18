package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson/primitive"
)

var entryId *primitive.ObjectID
var queue *Queue

func TestNewQueue(t *testing.T) {
	t.Run("TestNewQueue", func(t *testing.T) {
		var err error
		if queue, err = NewQueue("test", "queue", connectionUri, "local.pem", time.Second*5); err != nil {
			t.Errorf("Unable to get queue struct - reason: %v", err)
		} else if queue == nil {
			t.Errorf("Unable to get queue struct- reason: queue is nil")
		}
	})
}

func TestEnqueue(t *testing.T) {
	t.Run("TestEnqueue", func(t *testing.T) {
		var err error
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if err = queue.Enqueue(ctx, "this is a test", 30); err != nil {
			t.Errorf("Unable to enqueue - reason: %v", err)
		}
	})
}

func TestDequeue(t *testing.T) {
	t.Run("TestDequeue", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if entry, err := queue.Dequeue(ctx); err != nil {
			t.Errorf("Unable to dequeue - reason: %v", err)
		} else if entry == nil {
			t.Errorf("Dequeue failed - reason: entry not returned")
		} else if entry.Payload != "this is a test" {
			t.Errorf("Dequeue failed - reason: message is not correct")
		} else if entry.Version == nil {
			t.Errorf("Dequeue failed - reason: version is nil")
		} else if entry.Created == nil {
			t.Errorf("Dequeue failed - reason: created is nil")
		} else if entry.Started == nil {
			t.Errorf("Dequeue failed - reason: started is nil")
		} else if entry.Dequeued == nil {
			t.Errorf("Dequeue failed - reason: dequeued is nil")
		} else if entry.Expire == nil {
			t.Errorf("Dequeue failed - reason: expire is nil")
		} else if entry.Visibility != 30 {
			t.Errorf("Dequeue failed - reason: visibility is off")
		} else {
			if err = entry.Done(context.TODO()); err != nil {
				t.Errorf("Unable to mark message as done - reason: %v", err)
			}
		}
	})
}

func TestListen(t *testing.T) {
	t.Run("TestListen", func(t *testing.T) {
		channel := queue.Listen(2)

		send := 1000
		received := 0

		closed := false
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			for i := 0; i < send; i++ {
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				queue.Enqueue(ctx, "somerandomid", 5)
			}
			wg.Done()
		}()

		go func() {
			for entry := range channel {
				received++
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				if err := entry.Done(ctx); err != nil {
					t.Errorf("Problem with done: %v", err)
				}

				if received == send {
					wg.Done()
				}
			}
			closed = true
		}()

		wg.Wait()
		queue.StopListen()
		if !closed {
			t.Errorf("Channel not closed")
		}

	})
}
