package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

const defaultTimeoutInSeconds = 10

func objectId() *primitive.ObjectID {
	id := primitive.NewObjectID()
	return &id
}

func docdbClient(connectionUri, caFile string) (*mongo.Client, error) {
	client, err := mongo.NewClientWithOptions(
		connectionUri,
		options.Client().SetSSL(
			&options.SSLOpt{
				Enabled:  true,
				Insecure: true,
				CaFile:   caFile,
			},
		),
	)

	if err != nil {
		return nil, fmt.Errorf("Unable to create new db client -  reason: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeoutInSeconds*time.Second)
	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("Unable to connect to db - reason: %v", err)
	}

	ctx, _ = context.WithTimeout(context.Background(), defaultTimeoutInSeconds*time.Second)
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("Unable to ping db - reason: %v", err)
	}

	return client, nil
}

func ensureIndex(collection *mongo.Collection, keys interface{}) error {
	if _, err := collection.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{Keys: keys},
	); err != nil {
		return fmt.Errorf(
			"Unable to create index on db: %s - collection: %s - reason: %v",
			collection.Database().Name(),
			collection.Name(),
			err,
		)
	}

	return nil
}
