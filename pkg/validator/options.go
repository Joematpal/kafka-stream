package validator

import "github.com/joematpal/kafka-stream/pkg/sink"

type Option interface {
	apply(*Validator) error
}

type applyOptionFunc func(*Validator) error

func (f applyOptionFunc) apply(s *Validator) error {
	return f(s)
}

func WitLogger(logr Logger) Option {
	return applyOptionFunc(func(s *Validator) error {
		s.logr = logr
		return nil
	})
}

func WithTopicHandlers(topicHandlers map[string]ValidateMesssageHandlerFunc) Option {
	return applyOptionFunc(func(v *Validator) error {

		if v.topicHandlers == nil {
			v.topicHandlers = map[topic]sink.MessageHandler{}
		}

		for topic, handler := range topicHandlers {
			v.topicHandlers[topic] = handler(v)
		}
		return nil
	})
}

func WithFactoryAddressValidator(addressValidator FactorAddressValidator) Option {
	return applyOptionFunc(func(v *Validator) error {
		v.addressValidator = addressValidator
		return nil
	})
}

func WithDataQualityStore(dataQualityStore DataQualityStore[DataQualityResults]) Option {
	return applyOptionFunc(func(v *Validator) error {
		v.dataQualityStore = dataQualityStore
		return nil
	})
}
