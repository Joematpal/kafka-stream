package validator

import (
	"time"
)

// ValidationSeverity represents the severity level of a validation issue
type ValidationSeverity string

const (
	SeverityInfo     ValidationSeverity = "INFO"
	SeverityWarning  ValidationSeverity = "WARNING"
	SeverityError    ValidationSeverity = "ERROR"
	SeverityCritical ValidationSeverity = "CRITICAL"
)

// Enhanced DataQualityResult - builds on existing schema with key additions
type EnhancedDataQualityResult struct {
	// Keep existing fields from original schema
	ValidationName            string     `json:"validation_name" db:"validation_name"`
	ValidationVersion         string     `json:"validation_version" db:"validation_version"`
	DataRecord                any        `json:"data_record" db:"data_record"`
	DataRecordID              string     `json:"data_record_id" db:"data_record_id"`
	DataRecordType            string     `json:"data_record_type" db:"data_record_type"`
	DataRecordSource          string     `json:"data_record_source" db:"data_record_source"`
	DataRecordAttribute       string     `json:"data_record_attribute" db:"data_record_attribute"`
	DataRecordAttributeKey    string     `json:"data_record_attribute_key" db:"data_record_attribute_key"`
	DataRecordCreatedAt       *time.Time `json:"data_record_created_at" db:"data_record_created_at"`
	DataRecordUpdatedAt       *time.Time `json:"data_record_updated_at" db:"data_record_updated_at"`
	DataRecordUpdatedBySystem *time.Time `json:"data_record_updated_by_system" db:"data_record_updated_by_system"`
	DataRecordCreatedBySystem *time.Time `json:"data_record_created_by_system" db:"data_record_created_by_system"`
	SuggestedValue            any        `json:"suggested_value" db:"suggested_value"`
	ValidationErrors          []string   `json:"validation_errors" db:"validation_errors"`
	ValidatedAt               time.Time  `json:"validated_at" db:"validated_at"`

	// Key additions for better functionality
	ValidationSeverity ValidationSeverity `json:"validation_severity" db:"validation_severity"`
	IsValid            bool               `json:"is_valid" db:"is_valid"`
	MessageTopic       string             `json:"message_topic" db:"message_topic"`
	ProcessingTimeMs   int                `json:"processing_time_ms" db:"processing_time_ms"`
}

// Keep the existing DataQualityResults type for backward compatibility
type DataQualityResults = any

// Enhanced schema that builds on the original
var EnhancedDataQualitySchema = `
	CREATE TABLE IF NOT EXISTS data_quality_results (
		-- Original fields (keeping existing structure)
		validation_name string,
		validation_version string,
		data_record jsonb,
		data_record_id string,
		data_record_type string,
		data_record_source string,
		data_record_attribute string,
		data_record_attribute_key string,
		data_record_created_at Timestamp,
		data_record_updated_at Timestamp,
		data_record_updated_by_system Timestamp,
		data_record_created_by_system Timestamp,
		suggested_value jsonb,
		validation_errors TEXT[],
		validated_at Timestamp,
		
		-- Key enhancements
		validation_severity VARCHAR(20) CHECK (validation_severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
		is_valid BOOLEAN NOT NULL DEFAULT false,
		message_topic VARCHAR(255) NOT NULL,
		processing_time_ms INTEGER,
		
		-- Basic indexes for performance
		INDEX idx_topic_validated_at (message_topic, validated_at),
		INDEX idx_is_valid_severity (is_valid, validation_severity)
	)
`

// Enhanced DataQualityStore interface
type EnhancedDataQualityStore interface {
	Store(result *EnhancedDataQualityResult) error
	GetQualityScore(topic string, timeWindow time.Duration) (float64, error)
}

// ValidationStats for basic reporting
type ValidationStats struct {
	Topic           string  `json:"topic"`
	TotalMessages   int64   `json:"total_messages"`
	ValidMessages   int64   `json:"valid_messages"`
	InvalidMessages int64   `json:"invalid_messages"`
	QualityScore    float64 `json:"quality_score"`
}
