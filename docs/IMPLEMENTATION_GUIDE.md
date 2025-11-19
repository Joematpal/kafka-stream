# Universal Validation Service - Implementation Guide

## Overview

This guide shows how to implement the universal validation service using the designed components. The service validates messages from Kafka (and future SQS) sources using configurable rules, enhanced address validation, and comprehensive observability.

## Key Components Created

### 1. Enhanced Data Quality Schema ([`pkg/validator/data_quality.go`](../pkg/validator/data_quality.go))
- **EnhancedDataQualityResult**: Builds on existing schema with key additions
- **ValidationSeverity**: INFO, WARNING, ERROR, CRITICAL levels
- **ValidationStats**: Aggregated statistics for reporting

### 2. Validation Rule Engine ([`pkg/validator/rules.go`](../pkg/validator/rules.go))
- **RuleEngine**: Manages and executes validation rules per topic
- **ValidationRule**: Configurable rules with operators (required, eq, ne, regex, type)
- **ValidationResult**: Detailed validation outcomes

### 3. ISO20022 Address Validation ([`pkg/validator/address.go`](../pkg/validator/address.go))
- **ISO20022Address**: Standard address format
- **EnhancedAddressValidator**: Comprehensive address validation
- **AddressValidationResult**: Detailed validation with suggestions

### 4. OTEL Observability ([`pkg/validator/observability.go`](../pkg/validator/observability.go))
- **ValidationMetrics**: Counters, histograms, gauges for monitoring
- **ValidationTracer**: Distributed tracing support
- **ObservableValidator**: Wrapper with built-in observability

### 5. Configuration Management ([`pkg/validator/config.go`](../pkg/validator/config.go))
- **ValidationServiceConfig**: Complete service configuration
- **ConfigManager**: Dynamic configuration loading and reloading

## Fixed Issues in Original Code

### 1. Logic Error in HandleMessage
**File**: [`pkg/validator/validator.go:102`](../pkg/validator/validator.go:102)

**Original (Broken)**:
```go
handler, ok := v.topicHandlers[topic]
if ok {
    return errors.New("topic not supported")  // WRONG: returns error when handler IS found
}
```

**Fixed**:
```go
handler, ok := v.topicHandlers[topic]
if !ok {
    return errors.New("topic not supported")  // CORRECT: returns error when handler NOT found
}
```

### 2. Incomplete Data Quality Store Usage
**File**: [`pkg/validator/validator.go:97`](../pkg/validator/validator.go:97)

**Original (Incomplete)**:
```go
v.dataQualityStore.Store("")  // Storing empty string
```

**Should be**:
```go
result := &EnhancedDataQualityResult{
    ValidationName: "message_validation",
    ValidationVersion: "1.0.0",
    DataRecord: parsedMessage,
    DataRecordID: generateID(),
    DataRecordType: getMessageType(msg),
    DataRecordSource: "kafka",
    MessageTopic: topic,
    IsValid: validationPassed,
    ValidationSeverity: severity,
    ValidatedAt: time.Now(),
}
v.dataQualityStore.Store(result)
```

## Implementation Example

### Step 1: Create Enhanced Validator

```go
package main

import (
    "context"
    "log"
    
    "github.com/joematpal/kafka-stream/pkg/validator"
)

func main() {
    // Load configuration
    configManager, err := validator.NewConfigManager("./config/validation.json")
    if err != nil {
        log.Fatal(err)
    }
    config := configManager.GetConfig()

    // Create rule engine
    ruleEngine := validator.NewRuleEngine()
    
    // Add validation rules for topic_a
    ruleEngine.AddRule("topic_a", validator.ValidationRule{
        ID:          "topic_a_required_id",
        Name:        "ID Required",
        Description: "Message must have an ID field",
        Severity:    validator.SeverityError,
        Field:       "id",
        Operator:    "required",
        Enabled:     true,
    })
    
    ruleEngine.AddRule("topic_a", validator.ValidationRule{
        ID:          "topic_a_type_check",
        Name:        "Type Validation",
        Description: "Message type must be string",
        Severity:    validator.SeverityWarning,
        Field:       "type",
        Operator:    "type",
        Value:       "string",
        Enabled:     true,
    })

    // Add validation rules for ebe_v11
    ruleEngine.AddRule("ebe_v11", validator.ValidationRule{
        ID:          "ebe_v11_address_required",
        Name:        "Address Required",
        Description: "EBE message must have address",
        Severity:    validator.SeverityCritical,
        Field:       "address",
        Operator:    "required",
        Enabled:     true,
    })

    // Create address validator
    addressValidator := validator.NewDefaultAddressValidator()

    // Create enhanced data quality store (implement based on your DB)
    dataQualityStore := NewPostgreSQLDataQualityStore(config.Storage.DataQualityStore)

    // Create base validator with enhanced components
    baseValidator, err := validator.NewValidator(
        validator.WithTopicHandlers(map[string]validator.ValidateMesssageHandlerFunc{
            "topic_a": createGenericValidationHandler(ruleEngine),
            "ebe_v11": createEBEValidationHandler(ruleEngine, addressValidator),
        }),
        validator.WithFactoryAddressValidator(addressValidator),
        validator.WithDataQualityStore(dataQualityStore),
        validator.WitLogger(logger),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Wrap with observability
    observableValidator, err := validator.NewObservableValidator(baseValidator, config.Observability)
    if err != nil {
        log.Fatal(err)
    }

    // Set up message sources (Kafka consumer, etc.)
    // This would integrate with your existing Kafka setup
    
    log.Println("Universal validation service started")
}
```

### Step 2: Create Custom Validation Handlers

```go
// Generic validation handler for topic_a
func createGenericValidationHandler(ruleEngine *validator.RuleEngine) validator.ValidateMesssageHandlerFunc {
    return func(v *validator.Validator) sink.MessageHandler {
        return sink.MessageHandlerFunc(func(ctx context.Context, msg sink.Message) error {
            topic := msg.GetTopic()
            body := msg.GetBody()
            defer body.Close()

            // Parse message
            var messageData map[string]interface{}
            decoder := json.NewDecoder(body)
            if err := decoder.Decode(&messageData); err != nil {
                return err
            }

            // Run validation rules
            results := ruleEngine.ValidateMessage(ctx, topic, messageData)
            
            // Process validation results
            for _, result := range results {
                if !result.IsValid {
                    // Store validation failure
                    dataQualityResult := &validator.EnhancedDataQualityResult{
                        ValidationName:    result.RuleName,
                        ValidationVersion: "1.0.0",
                        DataRecord:        messageData,
                        DataRecordID:      generateMessageID(),
                        DataRecordType:    "generic",
                        DataRecordSource:  "kafka",
                        MessageTopic:      topic,
                        IsValid:          false,
                        ValidationSeverity: result.Severity,
                        ValidationErrors:  []string{result.ErrorMessage},
                        ValidatedAt:       time.Now(),
                    }
                    
                    if err := v.dataQualityStore.Store(dataQualityResult); err != nil {
                        v.logr.Error("failed to store validation result", "error", err)
                    }
                }
            }

            return nil
        })
    }
}

// EBE validation handler with address validation
func createEBEValidationHandler(ruleEngine *validator.RuleEngine, addressValidator validator.EnhancedAddressValidator) validator.ValidateMesssageHandlerFunc {
    return func(v *validator.Validator) sink.MessageHandler {
        return sink.MessageHandlerFunc(func(ctx context.Context, msg sink.Message) error {
            topic := msg.GetTopic()
            body := msg.GetBody()
            defer body.Close()

            // Parse EBE message
            var ebeData map[string]interface{}
            decoder := json.NewDecoder(body)
            if err := decoder.Decode(&ebeData); err != nil {
                return err
            }

            // Run basic validation rules
            results := ruleEngine.ValidateMessage(ctx, topic, ebeData)
            
            // Enhanced address validation for EBE
            if addressData, exists := ebeData["address"]; exists {
                addressResult, err := addressValidator.ValidateISO20022Address(ctx, 
                    convertToISO20022Address(addressData))
                if err != nil {
                    v.logr.Error("address validation failed", "error", err)
                } else if !addressResult.IsValid {
                    // Store address validation failure
                    dataQualityResult := &validator.EnhancedDataQualityResult{
                        ValidationName:     "iso20022_address_validation",
                        ValidationVersion:  "1.0.0",
                        DataRecord:         ebeData,
                        DataRecordID:       generateMessageID(),
                        DataRecordType:     "ebe_v11",
                        DataRecordSource:   "kafka",
                        MessageTopic:       topic,
                        IsValid:           false,
                        ValidationSeverity: validator.SeverityError,
                        ValidationErrors:   addressResult.Errors,
                        ValidatedAt:        time.Now(),
                    }
                    
                    if err := v.dataQualityStore.Store(dataQualityResult); err != nil {
                        v.logr.Error("failed to store address validation result", "error", err)
                    }
                }
            }

            return nil
        })
    }
}
```

### Step 3: Configuration File Example

**config/validation.json**:
```json
{
  "service": {
    "instance_id": "validator-001",
    "name": "validation-service",
    "version": "1.0.0",
    "log_level": "info"
  },
  "sources": {
    "kafka": {
      "enabled": true,
      "brokers": ["localhost:9092"],
      "consumer_group": "validation-service",
      "topics": [
        {
          "name": "topic_a",
          "handlers": ["generic_validation"]
        },
        {
          "name": "ebe_v11", 
          "handlers": ["ebe_validation", "address_validation"]
        }
      ],
      "batch_size": 100,
      "timeout": "30s"
    },
    "sqs": {
      "enabled": false,
      "region": "us-east-1",
      "queues": [],
      "batch_size": 10,
      "timeout": "30s"
    }
  },
  "validation": {
    "rules_config_path": "./config/validation_rules.json",
    "address_validator": {
      "provider": "default",
      "iso20022_enabled": true,
      "timeout": "10s"
    },
    "parallel_processing": true,
    "max_workers": 10,
    "batch_size": 100,
    "timeout": "60s"
  },
  "storage": {
    "data_quality_store": {
      "type": "postgresql",
      "connection_string": "postgres://user:pass@localhost/validation_db",
      "table_name": "data_quality_results",
      "batch_size": 1000,
      "flush_interval": "30s"
    }
  },
  "observability": {
    "service_name": "validation-service",
    "service_version": "1.0.0",
    "enabled": true,
    "metrics_enabled": true,
    "tracing_enabled": true,
    "sample_rate": 0.1
  }
}
```

## Next Steps for Production

### 1. Implement Data Quality Store
Create a PostgreSQL implementation of `EnhancedDataQualityStore`:

```go
type PostgreSQLDataQualityStore struct {
    db *sql.DB
}

func (p *PostgreSQLDataQualityStore) Store(result *EnhancedDataQualityResult) error {
    // Implement PostgreSQL storage
}
```

### 2. Add SQS Support
When ready to add SQS, implement an SQS message source that conforms to the `sink.Message` interface.

### 3. Enhanced Monitoring
Set up Grafana dashboards using the OTEL metrics:
- Message processing rates by topic
- Validation error rates by severity
- Data quality scores over time
- Processing latency percentiles

### 4. Rule Management API
Create REST API endpoints for:
- Adding/updating validation rules
- Viewing validation statistics
- Managing configuration

## Benefits of This Design

1. **Incremental Enhancement**: Builds on existing code without breaking changes
2. **Flexible Rule Engine**: Easy to add new validation rules without code changes
3. **Comprehensive Observability**: Built-in metrics, tracing, and logging
4. **ISO20022 Compliance**: Proper address validation for financial standards
5. **Future-Proof**: Easy to add SQS and other message sources
6. **Production Ready**: Proper error handling, configuration management, and monitoring

The design provides a solid foundation for a universal validation service that can grow with your company's needs while maintaining high data quality standards.