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

// Queue is queue processing struct. Keep a handle to this struct for
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

// NewQueue creates new new queue struct. This method panics if the timeout
// is negative, or any of the string parameters has a length of zero.
func NewQueue(
	dbName,
	collectionName,
	connectionUri,
	caFile string,
	timeout time.Duration,
) (*Queue, error) {

	if timeout < 1 {
		panic(fmt.Sprintf("Negative timeout duration passed - received: %d", timeout))
	}

	if len(dbName) == 0 {
		panic("Empty dbName param passed")
	}

	if len(collectionName) == 0 {
		panic("Empty collectionName param passed")
	}

	if len(caFile) == 0 {
		panic("Empty caFile param passed")
	}

	client, err := docdbClient(connectionUri, caFile, timeout)
	if err != nil {
		return nil, err
	}

	collection := client.Database(dbName).Collection(collectionName)
	ensureIndexes(collection)

	queue := &Queue{collection: collection, client: client, timeout: timeout}

	go queue.visibility()

	go queue.failures()

	return queue, nil
}

// Size provides the total depth of the message queue.
func (q *Queue) Size(ctx context.Context) (int64, error) {
	return q.collection.CountDocuments(ctx, bson.D{}, options.Count())
}

// Dequeue pulls the next item off the queue (if available). You must call
// the Done function on the message when you are done processing or it will
// timeout and be made visible again. If not entries are available, nil is
// returned.
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

	msg, err := q.readyMsg(ctx, msg.Id, version, msg.Visibility)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (q *Queue) readyMsg(
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

func (q *Queue) reset(filter func() interface{}, projection interface{}) {
	throttle := throttle()
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()

	for {
		opts := options.Find()
		opts.SetProjection(projection)
		opts.SetNoCursorTimeout(true)
		cur, err := q.collection.Find(context.Background(), filter())
		if err != nil {
			log.Errorf(
				"Unable to find reset - db: %s - collection: %s - reason: %v",
				q.collection.Database().Name(),
				q.collection.Name(),
				err,
			)
			throttle(true)
			continue
		}

		found := 0
		for cur.Next(ctx) {
			found++
			msg := &QueueMessage{}

			if err = cur.Decode(msg); err != nil {
				log.Errorf(
					"Unable to decode reset - db: %s - collection: %s - reason: %v",
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

// visibility looks for situations where the Done method on the message has
// not been called and the visibility timeout has been breached. Messages in this
// state are reset so they can be picked up again by Dequeue.
func (q *Queue) visibility() {
	q.reset(
		func() interface{} {
			return bson.D{{"expire", bson.D{{"$lte", timeNowUtc()}}}}
		},
		bson.D{{"_id", 1}, {"version", 1}},
	)
}

// failures looks for situations where the message was dequeued, but for
// some reason, ready message was never called. The timeout on this type
// of failure is currently hard-coded to three minutes.
func (q *Queue) failures() {
	q.reset(
		func() interface{} {
			return bson.D{
				{
					"$and",
					bson.A{
						bson.D{{"dequeued", bson.D{{"$lte", timeNowUtc().Add(time.Duration(-3) * time.Minute)}}}},
						bson.D{{"started", nil}},
					},
				},
			}
		},
		bson.D{{"_id", 1}, {"version", 1}},
	)
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

	for q.listening() {

		if msg, err := q.dequeue(); err != nil {
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

func (q *Queue) dequeue() (*QueueMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()

	return q.Dequeue(ctx)
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

	q.mux.Lock()
	close(q.channel)
	q.mux.Unlock()
}

// Ensure that the proper indices are on the collection. This is performed once by each
// process when the queue is created.
func ensureIndexes(collection *mongo.Collection) {
	if err := ensureIndex(collection, bson.D{{"dequeued", 1}, {"created", 1}}); err != nil {
		log.Errorf("Unable to create dequeued/created index on collection: %s - reason: %v", collection.Name(), err)
	}

	if err := ensureIndex(collection, bson.D{{"_id", 1}, {"version", 1}}); err != nil {
		log.Errorf("Unable to create _id/version index on collection: %s - reason: %v", collection.Name(), err)
	}

	if err := ensureIndex(collection, bson.D{{"expire", 1}}); err != nil {
		log.Errorf("Unable to create expire index on collection: %s - reason: %v", collection.Name(), err)
	}

	if err := ensureIndex(collection, bson.D{{"dequeued", 1}, {"started", 1}}); err != nil {
		log.Errorf("Unable to create dequeued/started index on collection: %s - reason: %v", collection.Name(), err)
	}
}
