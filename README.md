
[![GoDoc](https://godoc.org/github.com/rnzsgh/documentdb-queue?status.svg)](https://godoc.org/github.com/rnzsgh/documentdb-queue) [![Go Report Card](https://goreportcard.com/badge/github.com/rnzsgh/documentdb-queue)](https://goreportcard.com/report/github.com/rnzsgh/documentdb-queue)

# Overview

A simple POC showcasing how you can use [Amazon DocumentDB (with MongoDB compatibility)](https://aws.amazon.com/documentdb/) as a message queue. This queue is designed for [at-least-once delivery](http://www.cloudcomputingpatterns.org/at_least_once_delivery/). With this in mind, it is important to bake in [idempotence](https://en.wikipedia.org/wiki/Idempotence) into your applications because messages can be delivered multiple times. When a message is enqueued, you must specify a timeout parameter which is approximately the maximum amount of time the queue will allow before the message is made available again to be dequeued. After a message is dequeued, the clock starts ticking on the visibility timeout and you must call the *Done* function on the message, or it will be delivered to another process.

## Note

This uses the [MongoDB Go Driver](https://github.com/mongodb/mongo-go-driver) which at the time this was written is currently
a [beta release](https://github.com/mongodb/mongo-go-driver/releases/tag/v0.3.0).

This library has not been run in production and is not ready for prime time. There are bugs and logic errors that will cause
production issues, so please only use as a reference of what is possible.

## Requirements

This has only been tested on [Go 1.11](https://golang.org/doc/go1.11) with MongoDB 3.6.9 (for local development) and docdb3.6 (cloud).

## Use

### NewQueue

In the NewQueue function call, the parameters are:
* database name
* collection name
* [connection string URI](https://docs.mongodb.com/manual/reference/connection-string/)
* SSL Certificate file
* [Context](https://golang.org/pkg/context/) timeout for background operations

```golang
if queue, err = NewQueue("test", "queue", connectionUri, "local.pem", time.Second*5); err != nil {
  // Error - failed to connect to the database, there was an issue with the URI or the pem file
}
```

### Enqueue

Once you have the queue client, you can add entries to the queue using the *Enqueue* function:

Parameters:
* The context
* The message payload (string)
* The visibility timeout

```golang
if err := queue.Enqueue(context.TODO(), "this is a test", 30); err != nil {
  // Handle the error
}
```

### Dequeue

To get the latest message from the queue, call the *Dequeue* function:

Parameters:
* The context

```golang
if msg, err := queue.Dequeue(context.TODO()); err != nil {
  // Handle the error
} else {
  // Process the message

  if err = msg.Done(context.TODO()); err != nil {
    // It is possible that the msg was deleted by another process.
    // If the database is unavailable, the message wil be processed again
    // later when it is available.
  }
}
```

### Listen

While the *Dequeue* method is available, it is highly recommended that you use the *Listen* function
on the queue struct, which returns a [channel](https://gobyexample.com/channels). Additionally,
the listen approach also includes throttling (via [exponential backoff](https://en.wikipedia.org/wiki/Exponential_backoff))
in case there are database errors or no messages in the queue. If you call the *Listen* function, then
you must call the *StopListen* function to close the channel and stop the [goroutine(s)](https://gobyexample.com/goroutines).

Parameters:
* The number of goroutine(s) to spawn that call Dequeue

```golang
channel := queue.Listen(2)

// This range over the channel will exit when you call StopListen, because
// the channel is closed
for msg := range channel {

  // Process the message

  if err := msg.Done(context.TODO()); err != nil {
    // Handle the error
  }
}

// When you are done using the message queue, stop listening
queue.StopListen()

```

