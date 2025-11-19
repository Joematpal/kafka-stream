# Universal Validation Service Design

## Overview

This document outlines the design for a universal validation service that processes messages from multiple sources (Kafka, SQS) and performs comprehensive data quality validation with observability and reporting capabilities.

## Current Implementation Analysis

### Issues Identified in Current Code

1. **Logic Error in HandleMessage**: Line 102 in [`validator.go`](pkg/validator/validator.go:102) has inverted logic - returns error when handler IS found
2. **Incomplete Data Quality Store**: Line 97 stores empty string instead of meaningful data
3. **Limited Message Source Support**: Only supports generic sink interface
4. **No Observability**: Missing OTEL integration for metrics and tracing
5. **Rigid Validation Logic**: Hard-coded validation handlers without rule engine
6. **Schema Limitations**: DataQualityResults schema lacks important fields

## Enhanced Architecture Design

### Core Components

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Message       │    │   Validation     │    │   Data Quality  │
│   Sources       │───>│   Service        │───>│   Store         │
│                 │    │                  │    │                 │
│ • Kafka         │    │ • Rule Engine    │    │ • PostgreSQL    │
│ • SQS           │    │ • Validators     │    │ • Time Series   │
│ • Future...     │    │ • Metrics        │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                v
                       ┌─────────────────┐
                       │   Observability |
                       │                 │
                       │ • OTEL Metrics  │
                       │ • Grafana       │
                       │ • New Relic     │
                       └─────────────────┘
```

### Message Flow Architecture

```mermaid
graph TB
    subgraph "Message Sources"
        K[Kafka Topics]
        S[SQS Queues]
        F[Future Sources]
    end
    
    subgraph "Message Routing"
        MR[Message Router]
        MA[Message Adapter]
    end
    
    subgraph "Validation Engine"
        VE[Validation Engine]
        RR[Rule Registry]
        VH[Validation Handlers]
    end
    
    subgraph "Address Validation"
        AV[Address Validator]
        ISO[ISO20022 Validator]
    end
    
    subgraph "Data Quality"
        DQS[Data Quality Store]
        DQR[Data Quality Results]
    end
    
    subgraph "Observability"
        OTEL[OTEL Collector]
        METRICS[Metrics]
        TRACES[Traces]
        LOGS[Structured Logs]
    end
    
    K --> MR
    S --> MR
    F --> MR
    
    MR --> MA
    MA --> VE
    
    VE --> RR
    VE --> VH
    VH --> AV
    VH --> ISO
    
    VE --> DQS
    DQS --> DQR
    
    VE --> OTEL
    OTEL --> METRICS
    OTEL --> TRACES
    OTEL --> LOGS
```

## Enhanced Data Quality Results Schema

### Recommended Schema Improvements

```sql
CREATE TABLE data_quality_results (
    -- Primary identification
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Validation metadata
    validation_name VARCHAR(255) NOT NULL,
    validation_version VARCHAR(50) NOT NULL,
    validation_rule_id VARCHAR(255),
    validation_severity VARCHAR(20) CHECK (validation_severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    
    -- Message/Record information
    message_id VARCHAR(255),
    message_key VARCHAR(255),
    message_partition INTEGER,
    message_offset BIGINT,
    data_record JSONB NOT NULL,
    data_record_id VARCHAR(255) NOT NULL,
    data_record_type VARCHAR(100) NOT NULL,
    data_record_source VARCHAR(100) NOT NULL,
    data_record_topic VARCHAR(255) NOT NULL,
    
    -- Field-level validation
    field_path VARCHAR(500),
    field_name VARCHAR(255),
    field_value JSONB,
    expected_type VARCHAR(50),
    actual_type VARCHAR(50),
    
    -- Validation results
    is_valid BOOLEAN NOT NULL DEFAULT false,
    validation_errors TEXT[],
    validation_warnings TEXT[],
    suggested_value JSONB,
    confidence_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Timestamps
    message_timestamp TIMESTAMPTZ,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- System tracking
    validator_instance_id VARCHAR(255),
    processing_duration_ms INTEGER,
    
    -- Indexing for performance
    INDEX idx_validation_name_version (validation_name, validation_version),
    INDEX idx_data_record_type_source (data_record_type, data_record_source),
    INDEX idx_validated_at (validated_at),
    INDEX idx_is_valid_severity (is_valid, validation_severity),
    INDEX idx_topic_partition_offset (data_record_topic, message_partition, message_offset)
);
```

### Schema Improvements Explained

1. **Added UUID Primary Key**: Better for distributed systems
2. **Validation Metadata**: Rule ID, severity levels for better categorization
3. **Message Context**: Kafka/SQS specific fields (partition, offset, key)
4. **Field-Level Validation**: Granular validation at field level
5. **Validation Results**: Boolean validity, confidence scoring
6. **Performance**: Strategic indexes for common query patterns
7. **System Tracking**: Instance ID and processing metrics

## Validation Rule Engine

### Rule Definition Structure

```go
type ValidationRule struct {
    ID          string                 `json:"id"`
    Name        string                 `json:"name"`
    Version     string                 `json:"version"`
    Description string                 `json:"description"`
    Severity    ValidationSeverity     `json:"severity"`
    Conditions  []ValidationCondition  `json:"conditions"`
    Actions     []ValidationAction     `json:"actions"`
    Enabled     bool                   `json:"enabled"`
    Tags        []string              `json:"tags"`
}

type ValidationCondition struct {
    Field    string      `json:"field"`
    Operator string      `json:"operator"` // eq, ne, gt, lt, regex, exists, etc.
    Value    interface{} `json:"value"`
    Type     string      `json:"type"`     // string, number, boolean, array, object
}

type ValidationAction struct {
    Type   string                 `json:"type"` // log, metric, store, suggest
    Config map[string]interface{} `json:"config"`
}
```

## OTEL Integration Design

### Metrics to Track

```go
// Validation metrics
var (
    ValidationCounter = otel.NewCounter(
        "validation_total",
        "Total number of validations performed",
        []string{"topic", "rule", "severity", "status"},
    )
    
    ValidationDuration = otel.NewHistogram(
        "validation_duration_seconds",
        "Time spent validating messages",
        []string{"topic", "rule"},
    )
    
    MessageProcessingRate = otel.NewGauge(
        "messages_processed_per_second",
        "Rate of message processing",
        []string{"source", "topic"},
    )
    
    DataQualityScore = otel.NewGauge(
        "data_quality_score",
        "Overall data quality score by topic",
        []string{"topic", "time_window"},
    )
)
```

### Trace Spans

```go
// Tracing structure
ctx, span := tracer.Start(ctx, "validation.process_message")
defer span.End()

span.SetAttributes(
    attribute.String("message.topic", topic),
    attribute.String("message.id", messageID),
    attribute.String("validation.rule", ruleName),
)
```

## ISO20022 Address Validation

### Enhanced Address Validator Interface

```go
type AddressValidator interface {
    ValidateAddress(ctx context.Context, addr Address) (*AddressValidationResult, error)
    ValidateISO20022Address(ctx context.Context, addr ISO20022Address) (*AddressValidationResult, error)
    SuggestCorrections(ctx context.Context, addr Address) ([]AddressSuggestion, error)
}

type ISO20022Address struct {
    AddressType     string `json:"address_type"`     // ADDR, PBOX, HOME, BIZZ
    Department      string `json:"department"`
    SubDepartment   string `json:"sub_department"`
    StreetName      string `json:"street_name"`
    BuildingNumber  string `json:"building_number"`
    PostCode        string `json:"post_code"`
    TownName        string `json:"town_name"`
    CountrySubDiv   string `json:"country_sub_div"`
    Country         string `json:"country"`          // ISO 3166-1 alpha-2
}
```

## Configuration Management

### Service Configuration

```yaml
validation_service:
  instance_id: "validator-001"
  
  sources:
    kafka:
      enabled: true
      brokers: ["localhost:9092"]
      consumer_group: "validation-service"
      topics:
        - name: "topic_a"
          handlers: ["generic_validation"]
        - name: "ebe_v11"
          handlers: ["ebe_validation", "address_validation"]
    
    sqs:
      enabled: false  # Future implementation
      region: "us-east-1"
      queues: []
  
  validation:
    rules_config_path: "./config/validation_rules.json"
    address_validator:
      provider: "smartystreets"  # or "google", "here"
      iso20022_enabled: true
    
    parallel_processing: true
    max_workers: 10
    batch_size: 100
  
  storage:
    data_quality_store:
      type: "postgresql"
      connection_string: "postgres://user:pass@localhost/db"
      table_name: "data_quality_results"
  
  observability:
    otel:
      enabled: true
      endpoint: "http://localhost:4317"
      service_name: "validation-service"
      service_version: "1.0.0"
    
    metrics:
      enabled: true
      interval: "30s"
    
    tracing:
      enabled: true
      sample_rate: 0.1
```

## Implementation Roadmap

### Phase 1: Core Validation Service
- [ ] Fix existing validator logic bugs
- [ ] Implement flexible message source abstraction
- [ ] Create rule engine foundation
- [ ] Add basic OTEL integration

### Phase 2: Enhanced Validation
- [ ] Implement ISO20022 address validation
- [ ] Add comprehensive rule engine
- [ ] Enhance data quality schema
- [ ] Add configuration management

### Phase 3: Advanced Features
- [ ] SQS source implementation
- [ ] Advanced metrics and dashboards
- [ ] Performance optimization
- [ ] Auto-scaling capabilities

### Phase 4: Production Readiness
- [ ] Comprehensive testing
- [ ] Documentation
- [ ] Deployment automation
- [ ] Monitoring and alerting

## Best Practices and Recommendations

### Performance Considerations
1. **Batch Processing**: Process messages in batches for better throughput
2. **Parallel Validation**: Use worker pools for concurrent validation
3. **Caching**: Cache validation rules and address lookup results
4. **Connection Pooling**: Efficient database connection management

### Reliability Patterns
1. **Circuit Breaker**: Protect against downstream failures
2. **Retry Logic**: Exponential backoff for transient failures
3. **Dead Letter Queues**: Handle permanently failed messages
4. **Health Checks**: Comprehensive service health monitoring

### Security Considerations
1. **Data Encryption**: Encrypt sensitive data in transit and at rest
2. **Access Control**: Role-based access to validation results
3. **Audit Logging**: Track all validation activities
4. **PII Handling**: Proper handling of personally identifiable information

This design provides a robust, scalable, and observable validation service that can grow with your company's needs while maintaining high data quality standards.