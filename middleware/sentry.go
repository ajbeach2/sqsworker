package middleware

import (
	"github.com/getsentry/sentry-go"
)

func SentryHandler(msg *string, err error) {
	if err != nil {
		sentry.CaptureException(err)
	}
}
