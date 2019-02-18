package queue

import (
	"fmt"
	"math"
	"time"
)

func timeNowUtc() *time.Time {
	now := time.Now().UTC()
	return &now
}

// Sleep time doubles each time the closure is called, up to max.
// Closure function returns the number of times it has been called.
// Max is the number of iterations (i.e., a sleep time of max * duration).
// If max or amount are less than one, this function panics.
func timeExponentialSleep(unit time.Duration, amount, max int) func(int) int {

	if max < 1 {
		panic(fmt.Sprintf("timeExponentialSleep max is less than one - received: %d", max))
	}

	if amount < 1 {
		panic(fmt.Sprintf("timeExponentialSleep amount is less than one - received: %d", amount))
	}

	called := 0
	return func(i int) int {

		if i == 0 {
			called = 0
		}

		if i >= max {
			i = max - 1
		}

		time.Sleep(time.Duration(int(math.Pow(2, float64(i)))*amount) * unit)

		called++
		return called
	}
}
