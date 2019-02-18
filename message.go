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

// QueueMessage is the queue message structure.
type QueueMessage struct {
	Id         *primitive.ObjectID `json:"id" bson:"_id"`
	Version    *primitive.ObjectID `json:"version" bson:"version"`
	Visibility int                 `json:"visibility" bson:"visibility"` // Visibility timeout is in seconds
	Created    *time.Time          `json:"created" bson:"created"`
	Payload    string              `json:"payload" bson:"payload"`
	Started    *time.Time          `json:"started" bson:"started"`
	Dequeued   *time.Time          `json:"dequeued" bson:"dequeued"`
	Expire     *time.Time          `json:"expire" bson:"expire"`
	queue      *Queue
}

// Done tries to delete the message from the queue. If the visibility expired and the entry was
// updated, the version will not match and this method will return an error. The error is simply
// informational because the entry will be made available for another worker/processor.
// Reminder, this queue is for idempotent workloads.
func (m *QueueMessage) Done(ctx context.Context) error {

	filter := bson.D{{"_id", m.Id}, {"version", m.Version}}

	if res, err := m.queue.collection.DeleteOne(ctx, filter); err != nil {
		return fmt.Errorf(
			"Unable to delete entry - db: %s - collection: %s - id: %s - version: %s - reason: %v",
			m.queue.collection.Database().Name(),
			m.queue.collection.Name(),
			m.Id.Hex(),
			m.Version.Hex(),
			err,
		)
	} else if res.DeletedCount != 1 {
		return fmt.Errorf(
			"Unable to delete entry - db: %s - collection: %s - id: %s - version: %s - reason: doc not found",
			m.queue.collection.Database().Name(),
			m.queue.collection.Name(),
			m.Id.Hex(),
			m.Version.Hex(),
		)
	}
	return nil
}

// If the entry visibility is expired, reset.
func (m *QueueMessage) reset(ctx context.Context) error {
	opts := options.Update()
	opts.SetUpsert(false)

	filter := bson.D{{"_id", m.Id}, {"version", m.Version}}
	update := bson.D{{"$set", bson.D{{"version", objectId()}, {"dequeued", nil}, {"started", nil}, {"expire", nil}}}}

	if res, err := m.queue.collection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf(
			"Unable to reset queue entry after expiration - db: %s - collection: %s - id: %s - version: %s - reason: %v",
			m.queue.collection.Database().Name(),
			m.queue.collection.Name(),
			m.Id.Hex(),
			m.Version.Hex(),
			err,
		)
	} else if res.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}

	return nil
}
