package sink

import (
	"context"
	"io"
)

type Message interface {
	GetTopic() string
	GetBody() io.ReadCloser
}

type MessageHandlerFunc func(ctx context.Context, msg Message) error

func (f MessageHandlerFunc) HandleMessage(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, msg Message) error
}
