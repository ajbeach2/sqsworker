package middleware

import (
	"fmt"
	"testing"
)

func TestError(t *testing.T) {
	message := "test"
	SentryHandler(&message, fmt.Errorf("This is an error"))
}
