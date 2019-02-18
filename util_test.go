package queue

import (
	"testing"
	"time"
)

func TestTimeNowUtc(t *testing.T) {
	t.Run("TestTimeNowUtc", func(t *testing.T) {
		now := timeNowUtc()
		if now == nil {
			t.Errorf("Unable to get now time - reason: nil value returned!")
		}
	})
}

func TestTimeExponentialSleep(t *testing.T) {
	t.Run("TestTimeExponentialSleep", func(t *testing.T) {

		t0 := timeNowUtc()
		iterations := 6

		var called int

		sleeper := timeExponentialSleep(time.Millisecond, 5, 5)

		for i := 0; i < iterations; i++ {
			called = sleeper(called)
		}

		t1 := timeNowUtc()

		if t1.Sub(*t0) > time.Millisecond*275 {
			t.Errorf("Exponential sleep - slept too much")
		}

		if t1.Sub(*t0) < time.Millisecond*235 {
			t.Errorf("Exponential sleep - did not sleep enough")
		}

		if called != iterations {
			t.Errorf("Exponential sleep did not increment the counter properly")
		}
		return
	})
}
