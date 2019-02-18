package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

// The queue entry structure.
type QueueEntry struct {
	Id         *primitive.ObjectID `json:"id" bson:"_id"`
	Version    *primitive.ObjectID `json:"version" bson:"version"`
	Visibility int                 `json:"visibility" bson:"visibility"` // Visibility timeout is in seconds
	Created    *time.Time          `json:"created" bson:"created"`
	Payload    string              `json:"payload" bson:"payload"`
	Started    *time.Time          `json:"started" bson:"started"`
	Dequeued   *time.Time          `json:"dequeued" bson:"dequeued"`
	Expire     *time.Time          `json:"expire" bson:"expire"`
	queue      *Queue              `json:"-" bson:"-"`
}

// Try to delete the object. If the visibility expired and the entry was updated, the
// version will not match and this method will return an error. The error is simply
// informational because the entry will be made available for another worker/processor.
// Reminder, this queue is for idempotent workloads.
func (e *QueueEntry) Done(ctx context.Context) error {

	filter := bson.D{{"_id", e.Id}, {"version", e.Version}}

	if res, err := e.queue.collection.DeleteOne(ctx, filter); err != nil {
		return fmt.Errorf(
			"Unable to delete entry - db: %s - collection: %s - id: %s - version: %s - reason: %v",
			e.queue.collection.Database().Name(),
			e.queue.collection.Name(),
			e.Id.Hex(),
			e.Version.Hex(),
			err,
		)
	} else if res.DeletedCount != 1 {
		return fmt.Errorf(
			"Unable to delete entry - db: %s - collection: %s - id: %s - version: %s - reason: doc not found",
			e.queue.collection.Database().Name(),
			e.queue.collection.Name(),
			e.Id.Hex(),
			e.Version.Hex(),
		)
	}
	return nil
}

// If the entry visibility is expired, reset.
func (e *QueueEntry) reset(ctx context.Context) error {
	opts := options.Update()
	opts.SetUpsert(false)

	filter := bson.D{{"_id", e.Id}, {"version", e.Version}}
	update := bson.D{{"$set", bson.D{{"version", objectId()}, {"dequeued", nil}, {"started", nil}, {"expire", nil}}}}

	if res, err := e.queue.collection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf(
			"Unable to reset queue entry after expiration - db: %s - collection: %s - id: %s - version: %s - reason: %v",
			e.queue.collection.Database().Name(),
			e.queue.collection.Name(),
			e.Id.Hex(),
			e.Version.Hex(),
			err,
		)
	} else if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}
