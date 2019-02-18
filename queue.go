package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

// The queue struct. Keep a handle to this struct for
// sending and receiving messages.
type Queue struct {
	collection *mongo.Collection
	client     *mongo.Client
	mux        sync.Mutex
	wg         sync.WaitGroup
	channel    chan *QueueMessage
	running    bool
	timeout    time.Duration
}

// NewQueue creates new new queue struct.
func NewQueue(
	dbName,
	collectionName,
	connectionUri,
	caFile string,
	timeout time.Duration,
) (*Queue, error) {

	client, err := docdbClient(connectionUri, caFile, timeout)
	if err != nil {
		return nil, err
	}

	collection := client.Database(dbName).Collection(collectionName)
	ensureIndexes(collection)

	queue := &Queue{collection: collection, client: client, timeout: timeout}

	go queue.visibility()

	return queue, nil
}

// Size provides the total depth of the message queue.
func (q *Queue) Size(ctx context.Context) (int64, error) {
	return q.collection.CountDocuments(ctx, bson.D{}, options.Count())
}

// Pull the next item off the queue. You must call the Done function on
// the message when you are done processing or it will timeout and be made
// visible again. If not entries are available, nil is returned.
func (q *Queue) Dequeue(
	ctx context.Context,
) (*QueueMessage, error) {

	opts := options.FindOneAndUpdate()
	opts.SetReturnDocument(options.After)
	opts.SetSort(bson.D{{"created", 1}})
	opts.SetUpsert(false)
	opts.SetProjection(bson.D{{"_id", 1}, {"visibility", 1}})

	version := objectId()
	res := q.collection.FindOneAndUpdate(
		ctx,
		bson.D{{"dequeued", nil}},
		bson.D{{"$set", bson.D{{"version", version}, {"dequeued", timeNowUtc()}}}},
		opts,
	)

	if res.Err() != nil {
		err := fmt.Errorf(
			"Unable to dequeue - db: %s - collection: %s - reason: %v",
			q.collection.Database().Name(),
			q.collection.Name(),
			res.Err(),
		)
		return nil, err
	}

	msg := &QueueMessage{}
	if err := res.Decode(msg); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}

		e := fmt.Errorf(
			"Unable to decode in dequeue - db: %s - collection: %s - reason: %v",
			q.collection.Database().Name(),
			q.collection.Name(),
			err,
		)
		return nil, e
	}

	if msg, err := q.readyEntry(ctx, msg.Id, version, msg.Visibility); err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}

func (q *Queue) readyEntry(
	ctx context.Context,
	id *primitive.ObjectID,
	version *primitive.ObjectID,
	visibility int,
) (*QueueMessage, error) {

	opts := options.FindOneAndUpdate()
	opts.SetReturnDocument(options.After)
	opts.SetUpsert(false)

	now := timeNowUtc()
	expire := now.Add(time.Second * time.Duration(visibility))

	res := q.collection.FindOneAndUpdate(
		ctx,
		bson.D{{"_id", id}, {"version", version}},
		bson.D{{"$set", bson.D{{"started", now}, {"expire", expire}, {"version", objectId()}}}},
		opts,
	)

	if res.Err() != nil {
		err := fmt.Errorf(
			"Unable to update ready msg - db: %s - collection: %s - id %s - reason: %v",
			q.collection.Database().Name(),
			q.collection.Name(),
			id.Hex(),
			res.Err(),
		)
		return nil, err
	}

	msg := &QueueMessage{}

	if err := res.Decode(msg); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}

		e := fmt.Errorf(
			"Unable to decode msg in dequeue - db: %s - collection: %s - reason: %v",
			q.collection.Database().Name(),
			q.collection.Name(),
			err,
		)
		return nil, e
	}

	msg.queue = q

	return msg, nil
}

func (q *Queue) visibility() {
	throttle := throttle()
	for {

		opts := options.Find()
		opts.SetProjection(bson.D{{"_id", 1}, {"version", 1}})
		opts.SetNoCursorTimeout(true)
		cur, err := q.collection.Find(context.Background(), bson.D{{"expire", bson.D{{"$lte", timeNowUtc()}}}})
		if err != nil {
			log.Errorf(
				"Unable to find expired - db: %s - collection: %s - reason: %v",
				q.collection.Database().Name(),
				q.collection.Name(),
				err,
			)
			throttle(true)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
		defer cancel()
		found := 0
		for cur.Next(ctx) {
			found++
			msg := &QueueMessage{}

			if err = cur.Decode(msg); err != nil {
				log.Errorf(
					"Unable to decode expired - db: %s - collection: %s - reason: %v",
					q.collection.Database().Name(),
					q.collection.Name(),
					err,
				)
				throttle(true)
				continue
			} else {
				throttle(false)
				msg.queue = q
				if err = msg.reset(ctx); err != nil {
					if err != mongo.ErrNoDocuments {
						log.Error(err)
						throttle(true)
					}
				}
			}
		}
		cur.Close(context.Background())
		if found == 0 {
			throttle(true)
		}
	}
}

func (q *Queue) resetEntry() {

}

// Enqueue inserts a new item into the queue. This allows for an empty payload.
// If visibility is negative, this will panic.
func (q *Queue) Enqueue(
	ctx context.Context,
	payload string,
	visibility int,
) error {

	if visibility < 0 {
		panic("Cannot have a negative visibility timeout")
	}

	msg := &QueueMessage{
		Id:         objectId(),
		Version:    objectId(),
		Created:    timeNowUtc(),
		Payload:    payload,
		Visibility: visibility,
	}

	if _, err := q.collection.InsertOne(ctx, msg); err != nil {
		return fmt.Errorf("Unable to enqueue msg into collection %s - reason: %v", q.collection.Name(), err)
	}
	return nil
}

// Listen returns a channel and polls the database for new messages in
// separate goroutine(s). The channel created does not buffer. If you call Listen,
// you must call StopListen on process shutdown, which will close the channel.
// The count param indicates the number of goroutines to spawn to query the
// database for new entries. If count is less than 1, this method panics.
func (q *Queue) Listen(count int) <-chan *QueueMessage {

	if count < 1 {
		panic(fmt.Sprintf("Listen on queue with count < 1 - received: %d", count))
	}

	if q.listening() {
		return q.channel
	}

	q.mux.Lock()
	defer q.mux.Unlock()
	if q.channel != nil {
		return q.channel
	}

	q.channel = make(chan *QueueMessage)
	q.running = true

	q.wg.Add(count)
	for i := 0; i < count; i++ {
		go q.listen()
	}

	return q.channel
}

func (q *Queue) listening() bool {
	q.mux.Lock()
	defer q.mux.Unlock()
	return q.running
}

func throttle() func(bool) {
	misses := 0
	sleeper := timeExponentialSleep(time.Millisecond, 60, 6)

	return func(throttle bool) {
		if !throttle {
			misses = 0
		} else {
			misses = sleeper(misses)
		}
	}
}

func (q *Queue) listen() {
	throttle := throttle()
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()
	for q.listening() {
		if msg, err := q.Dequeue(ctx); err != nil {
			log.Error(err)
			throttle(true)
		} else if msg != nil {
			q.channel <- msg
			throttle(false)
		} else {
			throttle(true)
		}
	}
	q.wg.Done()
}

// StopListen must be called when you are ready to shutdown the Listen call. This
// closes the channel returned by the Listen call and terminates the goroutines that
// are call Dequeue.
func (q *Queue) StopListen() {
	q.mux.Lock()
	if q.channel != nil {
		q.running = false
	}
	q.mux.Unlock()

	q.wg.Wait()
	close(q.channel)
}

// Ensure that the proper indices are on the collection. This is performed once by each
// process when the queue is created.
func ensureIndexes(collection *mongo.Collection) {
	if err := ensureIndex(collection, bson.D{{"dequeued", 1}, {"created", 1}}); err != nil {
		log.Errorf("Unable to create dequeued index on collection: %s - reason: %v", collection.Name(), err)
	}

	if err := ensureIndex(collection, bson.D{{"_id", 1}, {"version", 1}}); err != nil {
		log.Errorf("Unable to create _id/version index on collection: %s - reason: %v", collection.Name(), err)
	}

	if err := ensureIndex(collection, bson.D{{"expire", 1}}); err != nil {
		log.Errorf("Unable to create expire index on collection: %s - reason: %v", collection.Name(), err)
	}
}
