package validator

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/joematpal/kafka-stream/pkg/sink"
)

type FactorAddressValidator interface {
	ValidateAddress(any) error
}

type NoOpLogger struct{}

func (l NoOpLogger) Debug(string, ...any) {

}
func (l NoOpLogger) Info(string, ...any) {

}
func (l NoOpLogger) Warn(string, ...any) {

}
func (l NoOpLogger) Error(string, ...any) {

}

type Logger interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

// DataQualityResults moved to data_quality.go to avoid redeclaration

type DataQualityStore[T DataQualityResults] interface {
	Store(T) error
}

type topic = string

type Validator struct {
	addressValidator FactorAddressValidator
	topicHandlers    map[topic]sink.MessageHandler

	dataQualityStore DataQualityStore[DataQualityResults]
	logr             Logger
}

func NewValidator(opts ...Option) (*Validator, error) {
	v := &Validator{}

	for _, opt := range opts {
		if err := opt.apply(v); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func (v *Validator) HandleMessage(ctx context.Context, msg sink.Message) error {

	body := msg.GetBody()

	defer body.Close()

	topic := msg.GetTopic()

	// TODO handle some rules validations; then publish to otel for dashboards and save to store.
	// This validation we want to be fairly generic or univseral; we want a way to valiate and message on a topic.
	//TODO: we wany to store something if some thing happened here....
	v.dataQualityStore.Store("")

	handler, ok := v.topicHandlers[topic]

	if !ok {
		return errors.New("topic not supported")
	}

	return handler.HandleMessage(ctx, msg)
}

type ValidateMesssageHandlerFunc func(v *Validator) sink.MessageHandler

// this struct always goes with this topic "ebe_v11"
type EBEv11 struct {
}

var EBEv11MessageHandlerFunc = ValidateMesssageHandlerFunc(func(v *Validator) sink.MessageHandler {
	return sink.MessageHandlerFunc(func(ctx context.Context, msg sink.Message) error {
		var out EBEv11
		topic := msg.GetTopic()
		body := msg.GetBody()
		defer body.Close()

		v.logr.Error("validating", "topic", topic)
		decoder := json.NewDecoder(body)
		if err := decoder.Decode(&out); err != nil {
			return err
		}

		if err := v.addressValidator.ValidateAddress(out); err != nil {
			v.logr.Error("validate address", "error", err)

		}

		if v.dataQualityStore != nil {
			if err := v.dataQualityStore.Store(out); err != nil {
				v.logr.Error("store", "error", err)
				return err
			}
		}
		return nil
	})
})
