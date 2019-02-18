package queue

import (
	"fmt"
	"testing"
)

var connectionUri = fmt.Sprintf("mongodb://%s:%s@%s:%s/work?ssl=true", "test", "test", "127.0.0.1", "27017")

func TestDocdbClient(t *testing.T) {
	t.Run("TestDocdbClient", func(t *testing.T) {
		if _, err := docdbClient(connectionUri, "local.pem"); err != nil {
			t.Errorf("Unable to get db client - reason: %v", err)
		}
	})
}
