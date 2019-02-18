
# Overview

A simple POC showcasing how you can use [Amazon DocumentDB (with MongoDB compatibility)](https://aws.amazon.com/documentdb/) as a job queue. This queue is designed for [at-least-once delivery](http://www.cloudcomputingpatterns.org/at_least_once_delivery/). With this in mind, it is important to bake in [idempotence](https://en.wikipedia.org/wiki/Idempotence) into your applications because messages can be delivered multiple times. When an entry is enqueued, you must specify a timeout parameter which is approximately the maximum amount of time the queue will allow before the message is made available again to be dequeued. After an entry is dequeued, the clock starts ticking on the visibility timeout and you must call the **Done** function on the entry, or it may be delivered to another process.

## Note

This uses the [MongoDB Go Driver](https://github.com/mongodb/mongo-go-driver) which at the time this was written is currently
a [beta release](https://github.com/mongodb/mongo-go-driver/releases/tag/v0.3.0).

## Requirements

This has only been tested on [Go 1.11](https://golang.org/doc/go1.11) with MongoDB 3.6.9 (for local development) and docdb3.6 (cloud).

## Use

In the following NewQueue function call, the parameters are:
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

Once you have the queue client, you can add entries to the queue using the Enqueue function.

The parameters are:
* The context object
* The message payload (string)
* The visibility timeout

```golang
ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
if err := queue.Enqueue(ctx, "this is a test", 30); err != nil {
  // Handle the error
}
```

