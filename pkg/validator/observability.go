package validator

import (
	"context"
	"time"

	"github.com/joematpal/kafka-stream/pkg/sink"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ValidationMetrics holds all the metrics for the validation service
type ValidationMetrics struct {
	MessagesProcessed  metric.Int64Counter
	ValidationDuration metric.Float64Histogram
	ValidationErrors   metric.Int64Counter
	DataQualityScore   metric.Float64Gauge
	ActiveValidations  metric.Int64UpDownCounter
}

// ValidationTracer holds tracing functionality
type ValidationTracer struct {
	tracer trace.Tracer
}

// ObservabilityConfig configures observability settings
type ObservabilityConfig struct {
	ServiceName    string
	ServiceVersion string
	Enabled        bool
	MetricsEnabled bool
	TracingEnabled bool
	SampleRate     float64
}

// NewValidationMetrics creates a new ValidationMetrics instance
func NewValidationMetrics() (*ValidationMetrics, error) {
	meter := otel.Meter("validation-service")

	messagesProcessed, err := meter.Int64Counter(
		"validation_messages_processed_total",
		metric.WithDescription("Total number of messages processed"),
	)
	if err != nil {
		return nil, err
	}

	validationDuration, err := meter.Float64Histogram(
		"validation_duration_seconds",
		metric.WithDescription("Time spent validating messages"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	validationErrors, err := meter.Int64Counter(
		"validation_errors_total",
		metric.WithDescription("Total number of validation errors"),
	)
	if err != nil {
		return nil, err
	}

	dataQualityScore, err := meter.Float64Gauge(
		"data_quality_score",
		metric.WithDescription("Current data quality score by topic"),
	)
	if err != nil {
		return nil, err
	}

	activeValidations, err := meter.Int64UpDownCounter(
		"validation_active_count",
		metric.WithDescription("Number of currently active validations"),
	)
	if err != nil {
		return nil, err
	}

	return &ValidationMetrics{
		MessagesProcessed:  messagesProcessed,
		ValidationDuration: validationDuration,
		ValidationErrors:   validationErrors,
		DataQualityScore:   dataQualityScore,
		ActiveValidations:  activeValidations,
	}, nil
}

// NewValidationTracer creates a new ValidationTracer
func NewValidationTracer(serviceName string) *ValidationTracer {
	tracer := otel.Tracer(serviceName)
	return &ValidationTracer{
		tracer: tracer,
	}
}

// StartValidationSpan starts a new tracing span for validation
func (vt *ValidationTracer) StartValidationSpan(ctx context.Context, operationName string, topic string) (context.Context, trace.Span) {
	return vt.tracer.Start(ctx, operationName,
		trace.WithAttributes(
			attribute.String("validation.topic", topic),
			attribute.String("validation.operation", operationName),
		),
	)
}

// RecordMessageProcessed records a processed message metric
func (vm *ValidationMetrics) RecordMessageProcessed(ctx context.Context, topic string, status string) {
	vm.MessagesProcessed.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status),
		),
	)
}

// RecordValidationDuration records validation duration
func (vm *ValidationMetrics) RecordValidationDuration(ctx context.Context, topic string, duration time.Duration) {
	vm.ValidationDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// RecordValidationError records a validation error
func (vm *ValidationMetrics) RecordValidationError(ctx context.Context, topic string, errorType string, severity string) {
	vm.ValidationErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("error_type", errorType),
			attribute.String("severity", severity),
		),
	)
}

// UpdateDataQualityScore updates the data quality score gauge
func (vm *ValidationMetrics) UpdateDataQualityScore(ctx context.Context, topic string, score float64) {
	vm.DataQualityScore.Record(ctx, score,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// IncrementActiveValidations increments active validation counter
func (vm *ValidationMetrics) IncrementActiveValidations(ctx context.Context, topic string) {
	vm.ActiveValidations.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// DecrementActiveValidations decrements active validation counter
func (vm *ValidationMetrics) DecrementActiveValidations(ctx context.Context, topic string) {
	vm.ActiveValidations.Add(ctx, -1,
		metric.WithAttributes(
			attribute.String("topic", topic),
		),
	)
}

// ObservableValidator wraps a validator with observability
type ObservableValidator struct {
	validator *Validator
	metrics   *ValidationMetrics
	tracer    *ValidationTracer
	config    ObservabilityConfig
}

// NewObservableValidator creates a new validator with observability
func NewObservableValidator(validator *Validator, config ObservabilityConfig) (*ObservableValidator, error) {
	var metrics *ValidationMetrics
	var tracer *ValidationTracer
	var err error

	if config.MetricsEnabled {
		metrics, err = NewValidationMetrics()
		if err != nil {
			return nil, err
		}
	}

	if config.TracingEnabled {
		tracer = NewValidationTracer(config.ServiceName)
	}

	return &ObservableValidator{
		validator: validator,
		metrics:   metrics,
		tracer:    tracer,
		config:    config,
	}, nil
}

// HandleMessage wraps the validator's HandleMessage with observability
func (ov *ObservableValidator) HandleMessage(ctx context.Context, msg sink.Message) error {
	if !ov.config.Enabled {
		return ov.validator.HandleMessage(ctx, msg)
	}

	topic := msg.GetTopic()
	start := time.Now()

	// Start tracing span
	var span trace.Span
	if ov.config.TracingEnabled && ov.tracer != nil {
		ctx, span = ov.tracer.StartValidationSpan(ctx, "validation.handle_message", topic)
		defer span.End()
	}

	// Increment active validations
	if ov.config.MetricsEnabled && ov.metrics != nil {
		ov.metrics.IncrementActiveValidations(ctx, topic)
		defer ov.metrics.DecrementActiveValidations(ctx, topic)
	}

	// Execute validation
	err := ov.validator.HandleMessage(ctx, msg)

	// Record metrics
	if ov.config.MetricsEnabled && ov.metrics != nil {
		duration := time.Since(start)
		ov.metrics.RecordValidationDuration(ctx, topic, duration)

		if err != nil {
			ov.metrics.RecordMessageProcessed(ctx, topic, "error")
			ov.metrics.RecordValidationError(ctx, topic, "processing_error", "ERROR")
		} else {
			ov.metrics.RecordMessageProcessed(ctx, topic, "success")
		}
	}

	// Add span attributes
	if span != nil {
		span.SetAttributes(
			attribute.String("validation.result", func() string {
				if err != nil {
					return "error"
				}
				return "success"
			}()),
			attribute.Int64("validation.duration_ms", time.Since(start).Milliseconds()),
		)

		if err != nil {
			span.RecordError(err)
		}
	}

	return err
}
