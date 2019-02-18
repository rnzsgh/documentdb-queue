package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
)

var msgId *primitive.ObjectID
var queue *Queue

func TestNewQueue(t *testing.T) {
	t.Run("TestNewQueue", func(t *testing.T) {
		var err error
		if queue, err = NewQueue("test", "queue", connectionUri, "local.pem", time.Second*5); err != nil {
			t.Errorf("Unable to get queue struct - reason: %v", err)
		} else if queue == nil {
			t.Errorf("Unable to get queue struct- reason: queue is nil")
		}

		if _, err = queue.collection.DeleteMany(context.Background(), bson.D{}); err != nil {
			t.Errorf("Failed to clear collection for testing - reason: %v", err)
		}
	})
}

func TestEnqueue(t *testing.T) {
	t.Run("TestEnqueue", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := queue.Enqueue(ctx, "this is a test", 30); err != nil {
			t.Errorf("Unable to enqueue - reason: %v", err)
		}
	})
}

func TestDequeue(t *testing.T) {
	t.Run("TestDequeue", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if msg, err := queue.Dequeue(ctx); err != nil {
			t.Errorf("Unable to dequeue - reason: %v", err)
		} else if msg == nil {
			t.Errorf("Dequeue failed - reason: msg not returned")
		} else if msg.Payload != "this is a test" {
			t.Errorf("Dequeue failed - reason: message is not correct")
		} else if msg.Version == nil {
			t.Errorf("Dequeue failed - reason: version is nil")
		} else if msg.Created == nil {
			t.Errorf("Dequeue failed - reason: created is nil")
		} else if msg.Started == nil {
			t.Errorf("Dequeue failed - reason: started is nil")
		} else if msg.Dequeued == nil {
			t.Errorf("Dequeue failed - reason: dequeued is nil")
		} else if msg.Expire == nil {
			t.Errorf("Dequeue failed - reason: expire is nil")
		} else if msg.Visibility != 30 {
			t.Errorf("Dequeue failed - reason: visibility is off")
		} else {
			if err = msg.Done(context.TODO()); err != nil {
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for i := 0; i < send; i++ {

				queue.Enqueue(ctx, "somerandomid", 5)
			}
			wg.Done()
		}()

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for msg := range channel {
				received++

				if err := msg.Done(ctx); err != nil {
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
		time.Sleep(10 * time.Millisecond) // Wait for the loop to exit the flag to be set
		if !closed {
			t.Errorf("Channel not closed")
		}

	})
}

// This test is going to enqueue an msg with a low visibility timeout. Next
// it is going to dequeue the msg, wait and then verify the item is available
// to dequeue.
func TestMessageVisibilityTimeout(t *testing.T) {
	t.Run("TestMessageVisibilityTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := queue.Enqueue(ctx, "this is a test", 1); err != nil {
			t.Errorf("Unable to enqueue in msg reset - reason: %v", err)
		} else {
			if msg, err := queue.Dequeue(ctx); err != nil {
				t.Errorf("Unable to dequeue msg reset - reason: %v", err)
			} else if msg == nil {
				t.Error("Message reset failure - reason: no msg for dequeue")
			} else {
				id := msg.Id
				version := msg.Version
				time.Sleep(3 * time.Second) // Let the background process run and reset the expired msg
				if msg, err = queue.Dequeue(ctx); err != nil {
					t.Errorf("Unable to dequeue msg reset - reason: %v", err)
				} else if msg == nil {
					t.Error("Message reset failure - reason: no msg for dequeue - value was not reset")
				} else {
					if *msg.Id != *id {
						t.Error("Message reset failure - reason: msg dequeued is not one expected")
					}

					if *msg.Version == *version {
						t.Error("Message reset failure - reason: msg dequeued does not have an updated version")
					}

					if err := msg.Done(ctx); err != nil {
						t.Errorf("Message reset failure - reason: %v", err)
					}
				}
			}
		}
	})
}
