
# Overview

A simple POC showcasing how you can use [Amazon DocumentDB (with MongoDB compatibility)](https://aws.amazon.com/documentdb/) as a job queue.

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
