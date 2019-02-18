package queue

import (
	"context"
	"time"

	"github.com/mongodb/mongo-go-driver/bson/primitive"
)

// The queue entry structure.
type QueueEntry struct {
	Id         *primitive.ObjectID `json:"id" bson:"_id"`
	Version    *primitive.ObjectID `json:"version" bson:"version"`
	Visibility int                 `json:"visibility" bson:"visibility"` // Visibility timeout is in seconds
	Created    *time.Time          `json:"created" bson:"created"`
	Started    *time.Time          `json:"started" bson:"started"`
	Dequeued   *time.Time          `json:"dequeued" bson:"dequeued"`
	Expire     *time.Time          `json:"expire" bson:"expire"`
	Payload    string              `json:"payload" bson:"payload"`
	queue      *Queue              `json:"-" bson:"-"`
}

// Try to delete the object. If the visibility expired and the entry was updated, the
// version will not match and this method will return an error. The error is simply
// informational because the entry will be made available for another worker/processor.
// Reminder, this queue is for idempotent workloads.
func (e *QueueEntry) Done(ctx context.Context) error {
	return deleteQueueEntry(ctx, e.queue.collection, e.Id, e.Version)
}
