# Universal Validation Service Design

## Overview

This document outlines the design for a universal validation service that processes messages from multiple sources (Kafka, SQS) and performs comprehensive data quality validation with observability and reporting capabilities.

## Current Implementation Analysis

### Issues Identified in Current Code

1. **Incomplete Data Quality Store**: Line 75 stores empty string instead of meaningful validation data
2. **Limited Message Source Support**: Only supports generic sink interface
3. **Disconnected Observability**: OTEL integration exists but not integrated in main validation flow
4. **Unused Rule Engine**: RuleEngine struct exists but not integrated with main Validator
5. **Schema Limitations**: DataQualityResults is defined as `any` type, lacks structure
6. **Interface Mismatches**: Address validator interface differs between design and implementation

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

## Data Quality Schema Analysis

### Current Implementation Status

The current implementation has a mismatch between design and code:

```go
// Current implementation in pkg/validator/data_quality.go
type DataQualityResults = any  // Generic type alias - no structure

// Enhanced structure exists but not integrated:
type EnhancedDataQualityResult struct {
    ValidationName            string             `json:"validation_name"`
    ValidationVersion         string             `json:"validation_version"`
    DataRecord                any                `json:"data_record"`
    DataRecordID              string             `json:"data_record_id"`
    DataRecordType            string             `json:"data_record_type"`
    DataRecordSource          string             `json:"data_record_source"`
    ValidationSeverity        ValidationSeverity `json:"validation_severity"`
    IsValid                   bool               `json:"is_valid"`
    MessageTopic              string             `json:"message_topic"`
    ProcessingTimeMs          int                `json:"processing_time_ms"`
    ValidationErrors          []string           `json:"validation_errors"`
    ValidatedAt               time.Time          `json:"validated_at"`
    // ... other fields
}
```

### Recommended Schema for Production

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

## Current Implementation Analysis

### Existing Components

The current implementation includes these key components:

1. **Validator** ([`pkg/validator/validator.go`](pkg/validator/validator.go)) - Main validation orchestrator
2. **RuleEngine** ([`pkg/validator/rules.go`](pkg/validator/rules.go)) - Rule-based validation (not integrated)
3. **ObservableValidator** ([`pkg/validator/observability.go`](pkg/validator/observability.go)) - OTEL wrapper (not integrated)
4. **DataQualityStore** - Generic interface with `any` type
5. **FactorAddressValidator** - Address validation interface

### Separate protovalidate-go Implementation

The project also has a separate protovalidate-go implementation in [`cmd/validatepb/main.go`](cmd/validatepb/main.go):

```go
// Already implemented workflow:
// 1. Define schemas with buf.validate constraints in .proto files
// 2. Generate descriptors using buf generate
// 3. Load descriptors and validate messages using protovalidate-go

type ValidationService struct {
    validator   *protovalidate.Validator
    descriptors map[string]protoreflect.FileDescriptor
}

func (vs *ValidationService) ValidateKafkaMessage(topic string, jsonData []byte) error {
    // Convert JSON to dynamic protobuf message
    fd := vs.descriptors[topic]
    msgDesc := fd.Messages().ByName(getMessageTypeForTopic(topic))
    msg := dynamicpb.NewMessage(msgDesc)
    
    // Populate message from JSON
    if err := protojson.Unmarshal(jsonData, msg); err != nil {
        return fmt.Errorf("failed to unmarshal JSON: %w", err)
    }
    
    // Validate using existing protovalidate-go validator
    return vs.validator.Validate(msg)
}
```

### Schema-Based Validation Rules

Validation rules are defined in protobuf schemas using buf.validate constraints:

```protobuf
// pkg/user/v1/user.proto (already exists)
message User {
  string email = 1 [(buf.validate.field).string.email = true];
  int32 age = 2 [(buf.validate.field).int32 = {gte: 0, lte: 120}];
  string name = 3 [(buf.validate.field).string.min_len = 1];
}

// For Kafka topics, define message schemas:
message TopicAMessage {
  string id = 1 [(buf.validate.field).string.min_len = 1];
  string type = 2 [(buf.validate.field).string = {in: ["order", "payment", "notification"]}];
  google.protobuf.Timestamp timestamp = 3 [(buf.validate.field).timestamp.gte.seconds = 1577836800]; // >= 2020-01-01
}

message EBEv11Message {
  Address address = 1 [(buf.validate.field).required = true];
  Transaction transaction = 2 [(buf.validate.field).required = true];
}

message Address {
  string country = 1 [(buf.validate.field).string.len = 2]; // ISO 3166-1 alpha-2
  string town_name = 2 [(buf.validate.field).string.min_len = 1];
  string post_code = 3 [(buf.validate.field).string.pattern = "^[0-9]{5}(-[0-9]{4})?$"];
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

## Address Validation Implementation

### Current Address Validator Interface

```go
// Current implementation in pkg/validator/validator.go
type FactorAddressValidator interface {
    ValidateAddress(any) error
}
```

### Recommended Enhanced Interface

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

### Migration Path

1. **Current**: `FactorAddressValidator` with simple `ValidateAddress(any) error`
2. **Target**: Enhanced interface with context, structured types, and result objects
3. **Steps**: Gradually migrate from `any` type to structured address types

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

### Current State Assessment
- [x] Basic Validator structure with topic handlers
- [x] RuleEngine implementation (not integrated)
- [x] ObservableValidator with OTEL metrics (not integrated)
- [x] Enhanced data quality structures (not integrated)
- [x] Address validation interface (basic)
- [ ] Integration between components
- [ ] Proper data quality storage

### Phase 1: Integration and Alignment
- [ ] Integrate RuleEngine with main Validator
- [ ] Integrate ObservableValidator as default wrapper
- [ ] Replace `DataQualityResults = any` with structured type
- [ ] Fix data quality storage (remove empty string storage)
- [ ] Add proper error handling and logging

### Phase 2: Enhanced Validation
- [ ] Implement structured address validation
- [ ] Add ISO20022 address validation
- [ ] Enhance data quality schema implementation
- [ ] Add configuration management
- [ ] Integrate protovalidate-go with main validator

### Phase 3: Advanced Features
- [ ] SQS source implementation
- [ ] Advanced metrics and dashboards
- [ ] Performance optimization
- [ ] Auto-scaling capabilities

### Phase 4: Production Readiness
- [ ] Comprehensive testing
- [ ] Documentation alignment
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