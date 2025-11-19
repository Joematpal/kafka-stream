# Validation Service Flow Diagrams

## Overall System Architecture

```mermaid
graph TB
    subgraph "Message Sources"
        K[Kafka Brokers<br/>Topics: topic_a, ebe_v11]
        S[SQS Queues<br/>Future Implementation]
        F[Future Sources<br/>RabbitMQ, etc.]
    end
    
    subgraph "Message Ingestion Layer"
        KConsumer[Kafka Consumer<br/>Consumer Group: validation-service]
        SConsumer[SQS Consumer<br/>Polling/Long Polling]
        MRouter[Message Router<br/>Route by Source Type]
    end
    
    subgraph "Validation Service Core"
        VS[Validation Service<br/>Main Orchestrator]
        RuleEngine[Rule Engine<br/>Dynamic Rule Processing]
        ValidatorRegistry[Validator Registry<br/>Topic → Handler Mapping]
    end
    
    subgraph "Validation Handlers"
        GenericHandler[Generic Validator<br/>topic_a]
        EBEHandler[EBE v11 Validator<br/>ebe_v11 topic]
        AddressHandler[Address Validator<br/>Cross-cutting]
        ISO20022Handler[ISO20022 Validator<br/>Address Standards]
    end
    
    subgraph "Data Quality & Storage"
        DQEngine[Data Quality Engine<br/>Score Calculation]
        DQStore[(Data Quality Store<br/>PostgreSQL)]
        ResultsAPI[Results API<br/>Query Interface]
    end
    
    subgraph "Observability Stack"
        OTELCollector[OTEL Collector<br/>Metrics & Traces]
        Prometheus[Prometheus<br/>Metrics Storage]
        Grafana[Grafana<br/>Dashboards]
        NewRelic[New Relic<br/>APM & Alerts]
        Jaeger[Jaeger<br/>Distributed Tracing]
    end
    
    K --> KConsumer
    S --> SConsumer
    F --> MRouter
    
    KConsumer --> MRouter
    SConsumer --> MRouter
    
    MRouter --> VS
    VS --> RuleEngine
    VS --> ValidatorRegistry
    
    ValidatorRegistry --> GenericHandler
    ValidatorRegistry --> EBEHandler
    
    GenericHandler --> AddressHandler
    EBEHandler --> AddressHandler
    AddressHandler --> ISO20022Handler
    
    VS --> DQEngine
    DQEngine --> DQStore
    DQStore --> ResultsAPI
    
    VS --> OTELCollector
    RuleEngine --> OTELCollector
    DQEngine --> OTELCollector
    
    OTELCollector --> Prometheus
    OTELCollector --> Jaeger
    Prometheus --> Grafana
    OTELCollector --> NewRelic
```

## Message Processing Flow

```mermaid
sequenceDiagram
    participant K as Kafka Topic
    participant C as Consumer
    participant VS as Validation Service
    participant RE as Rule Engine
    participant VH as Validation Handler
    participant AV as Address Validator
    participant DQ as Data Quality Store
    participant OTEL as OTEL Collector
    participant G as Grafana

    K->>C: Message Available
    C->>VS: HandleMessage(ctx, msg)
    
    Note over VS: Extract topic, messageID, body
    VS->>OTEL: Start Span "validation.process_message"
    VS->>OTEL: Counter "messages_received"
    
    VS->>RE: GetValidationRules(topic)
    RE-->>VS: []ValidationRule
    
    loop For each validation rule
        VS->>VH: ValidateMessage(rule, msg)
        
        alt Rule requires address validation
            VH->>AV: ValidateAddress(addressData)
            AV-->>VH: ValidationResult
        end
        
        VH-->>VS: ValidationResult
        VS->>OTEL: Counter "validations_performed"
        VS->>OTEL: Histogram "validation_duration"
    end
    
    VS->>DQ: Store(DataQualityResult)
    DQ-->>VS: Success/Error
    
    VS->>OTEL: End Span
    VS->>OTEL: Gauge "data_quality_score"
    
    OTEL->>G: Metrics & Traces
    
    VS-->>C: Success/Error
    C-->>K: Commit Offset
```

## Topic-Specific Validation Flows

### Topic A (Generic) Validation Flow

```mermaid
flowchart TD
    Start([Message from topic_a]) --> Parse[Parse JSON Message]
    Parse --> Schema[Schema Validation]
    
    Schema -->|Valid| DataType[Data Type Validation]
    Schema -->|Invalid| LogError1[Log Schema Error]
    LogError1 --> StoreResult1[Store Validation Result]
    
    DataType -->|Valid| Business[Business Rule Validation]
    DataType -->|Invalid| LogError2[Log Data Type Error]
    LogError2 --> StoreResult2[Store Validation Result]
    
    Business -->|Valid| Success[Mark as Valid]
    Business -->|Invalid| LogError3[Log Business Rule Error]
    LogError3 --> StoreResult3[Store Validation Result]
    
    Success --> StoreSuccess[Store Success Result]
    StoreResult1 --> Metrics1[Update Metrics]
    StoreResult2 --> Metrics2[Update Metrics]
    StoreResult3 --> Metrics3[Update Metrics]
    StoreSuccess --> Metrics4[Update Metrics]
    
    Metrics1 --> End([End])
    Metrics2 --> End
    Metrics3 --> End
    Metrics4 --> End
```

### EBE v11 Validation Flow

```mermaid
flowchart TD
    Start([Message from ebe_v11]) --> ParseEBE[Parse EBE v11 JSON]
    ParseEBE --> SchemaEBE[EBE Schema Validation]
    
    SchemaEBE -->|Valid| ExtractAddr[Extract Address Data]
    SchemaEBE -->|Invalid| LogEBEError[Log EBE Schema Error]
    LogEBEError --> StoreEBEResult[Store Validation Result]
    
    ExtractAddr --> ValidateAddr[Address Validation]
    ValidateAddr --> ISO20022Check{ISO20022 Required?}
    
    ISO20022Check -->|Yes| ISO20022Val[ISO20022 Address Validation]
    ISO20022Check -->|No| StandardAddr[Standard Address Validation]
    
    ISO20022Val -->|Valid| AddrSuccess[Address Valid]
    ISO20022Val -->|Invalid| AddrError[Address Invalid]
    
    StandardAddr -->|Valid| AddrSuccess
    StandardAddr -->|Invalid| AddrError
    
    AddrSuccess --> BusinessEBE[EBE Business Rules]
    AddrError --> LogAddrError[Log Address Error]
    LogAddrError --> StoreAddrResult[Store Address Result]
    
    BusinessEBE -->|Valid| EBESuccess[Mark EBE as Valid]
    BusinessEBE -->|Invalid| LogBusinessError[Log Business Error]
    LogBusinessError --> StoreBusinessResult[Store Business Result]
    
    EBESuccess --> StoreEBESuccess[Store Success Result]
    StoreEBEResult --> MetricsEBE1[Update EBE Metrics]
    StoreAddrResult --> MetricsEBE2[Update EBE Metrics]
    StoreBusinessResult --> MetricsEBE3[Update EBE Metrics]
    StoreEBESuccess --> MetricsEBE4[Update EBE Metrics]
    
    MetricsEBE1 --> EndEBE([End])
    MetricsEBE2 --> EndEBE
    MetricsEBE3 --> EndEBE
    MetricsEBE4 --> EndEBE
```

## Address Validation Flow

```mermaid
flowchart TD
    Start([Address Data Input]) --> DetectType{Address Type Detection}
    
    DetectType -->|Standard| StandardFlow[Standard Address Flow]
    DetectType -->|ISO20022| ISO20022Flow[ISO20022 Address Flow]
    DetectType -->|Unknown| UnknownFlow[Unknown Format Flow]
    
    subgraph "Standard Address Validation"
        StandardFlow --> ParseStd[Parse Standard Fields]
        ParseStd --> ValidateStd[Validate Against Provider]
        ValidateStd --> StdResult{Validation Result}
        
        StdResult -->|Valid| StdSuccess[Standard Valid]
        StdResult -->|Invalid| StdSuggest[Generate Suggestions]
        StdSuggest --> StdInvalid[Standard Invalid]
    end
    
    subgraph "ISO20022 Address Validation"
        ISO20022Flow --> ParseISO[Parse ISO20022 Fields]
        ParseISO --> ValidateISO[Validate ISO20022 Format]
        ValidateISO --> ISOResult{Validation Result}
        
        ISOResult -->|Valid| ISOSuccess[ISO20022 Valid]
        ISOResult -->|Invalid| ISOSuggest[Generate ISO Suggestions]
        ISOSuggest --> ISOInvalid[ISO20022 Invalid]
    end
    
    subgraph "Unknown Format Handling"
        UnknownFlow --> AttemptParse[Attempt Best-Effort Parse]
        AttemptParse --> FallbackValidate[Fallback Validation]
        FallbackValidate --> UnknownResult[Unknown Format Result]
    end
    
    StdSuccess --> StoreAddr1[Store Address Result]
    StdInvalid --> StoreAddr2[Store Address Result]
    ISOSuccess --> StoreAddr3[Store Address Result]
    ISOInvalid --> StoreAddr4[Store Address Result]
    UnknownResult --> StoreAddr5[Store Address Result]
    
    StoreAddr1 --> AddrMetrics[Update Address Metrics]
    StoreAddr2 --> AddrMetrics
    StoreAddr3 --> AddrMetrics
    StoreAddr4 --> AddrMetrics
    StoreAddr5 --> AddrMetrics
    
    AddrMetrics --> EndAddr([End Address Validation])
```

## Data Quality Scoring Flow

```mermaid
flowchart TD
    Start([Validation Results]) --> Aggregate[Aggregate Results by Topic]
    
    Aggregate --> Calculate{Calculate Scores}
    
    Calculate --> FieldScore[Field-Level Scores]
    Calculate --> RecordScore[Record-Level Scores]
    Calculate --> TopicScore[Topic-Level Scores]
    
    FieldScore --> WeightField[Apply Field Weights]
    RecordScore --> WeightRecord[Apply Record Weights]
    TopicScore --> WeightTopic[Apply Topic Weights]
    
    WeightField --> CombineField[Combine Field Scores]
    WeightRecord --> CombineRecord[Combine Record Scores]
    WeightTopic --> CombineTopic[Combine Topic Scores]
    
    CombineField --> OverallScore[Calculate Overall Score]
    CombineRecord --> OverallScore
    CombineTopic --> OverallScore
    
    OverallScore --> Threshold{Score Thresholds}
    
    Threshold -->|>= 0.9| Excellent[Excellent Quality]
    Threshold -->|>= 0.7| Good[Good Quality]
    Threshold -->|>= 0.5| Fair[Fair Quality]
    Threshold -->|< 0.5| Poor[Poor Quality]
    
    Excellent --> UpdateDashboard[Update Quality Dashboard]
    Good --> UpdateDashboard
    Fair --> UpdateDashboard
    Poor --> TriggerAlert[Trigger Quality Alert]
    
    TriggerAlert --> UpdateDashboard
    UpdateDashboard --> StoreScores[Store Quality Scores]
    StoreScores --> End([End])
```

## Error Handling and Recovery Flow

```mermaid
flowchart TD
    Start([Processing Error Occurred]) --> ClassifyError{Classify Error Type}
    
    ClassifyError -->|Transient| TransientFlow[Transient Error Flow]
    ClassifyError -->|Permanent| PermanentFlow[Permanent Error Flow]
    ClassifyError -->|Unknown| UnknownFlow[Unknown Error Flow]
    
    subgraph "Transient Error Handling"
        TransientFlow --> RetryCheck{Retry Count < Max?}
        RetryCheck -->|Yes| BackoffWait[Exponential Backoff Wait]
        BackoffWait --> RetryProcess[Retry Processing]
        RetryProcess --> RetryResult{Retry Success?}
        
        RetryResult -->|Success| RecoverySuccess[Recovery Successful]
        RetryResult -->|Failure| RetryCheck
        RetryCheck -->|No| RetryExhausted[Retry Exhausted]
    end
    
    subgraph "Permanent Error Handling"
        PermanentFlow --> LogPermanent[Log Permanent Error]
        LogPermanent --> DeadLetter[Send to Dead Letter Queue]
        DeadLetter --> AlertPermanent[Alert Operations Team]
    end
    
    subgraph "Unknown Error Handling"
        UnknownFlow --> LogUnknown[Log Unknown Error]
        LogUnknown --> CircuitBreaker{Circuit Breaker Open?}
        CircuitBreaker -->|No| TreatTransient[Treat as Transient]
        CircuitBreaker -->|Yes| TreatPermanent[Treat as Permanent]
        
        TreatTransient --> TransientFlow
        TreatPermanent --> PermanentFlow
    end
    
    RecoverySuccess --> UpdateMetrics1[Update Recovery Metrics]
    RetryExhausted --> UpdateMetrics2[Update Failure Metrics]
    AlertPermanent --> UpdateMetrics3[Update Error Metrics]
    
    UpdateMetrics1 --> End([End])
    UpdateMetrics2 --> End
    UpdateMetrics3 --> End
```

## Observability and Monitoring Flow

```mermaid
flowchart TD
    Start([Service Operation]) --> CollectMetrics[Collect Metrics]
    CollectMetrics --> CollectTraces[Collect Traces]
    CollectTraces --> CollectLogs[Collect Structured Logs]
    
    CollectMetrics --> MetricTypes{Metric Types}
    MetricTypes --> Counters[Counters<br/>• Messages Processed<br/>• Validations Performed<br/>• Errors Occurred]
    MetricTypes --> Gauges[Gauges<br/>• Active Connections<br/>• Queue Depth<br/>• Quality Scores]
    MetricTypes --> Histograms[Histograms<br/>• Processing Duration<br/>• Message Size<br/>• Validation Time]
    
    CollectTraces --> TraceSpans[Trace Spans]
    TraceSpans --> MessageSpan[Message Processing Span]
    TraceSpans --> ValidationSpan[Validation Span]
    TraceSpans --> StorageSpan[Storage Span]
    
    CollectLogs --> LogLevels{Log Levels}
    LogLevels --> InfoLogs[INFO: Processing Events]
    LogLevels --> WarnLogs[WARN: Quality Issues]
    LogLevels --> ErrorLogs[ERROR: Processing Failures]
    LogLevels --> DebugLogs[DEBUG: Detailed Traces]
    
    Counters --> OTELExport[Export to OTEL Collector]
    Gauges --> OTELExport
    Histograms --> OTELExport
    MessageSpan --> OTELExport
    ValidationSpan --> OTELExport
    StorageSpan --> OTELExport
    InfoLogs --> OTELExport
    WarnLogs --> OTELExport
    ErrorLogs --> OTELExport
    DebugLogs --> OTELExport
    
    OTELExport --> Destinations{Export Destinations}
    Destinations --> Prometheus[Prometheus<br/>Metrics Storage]
    Destinations --> Jaeger[Jaeger<br/>Trace Storage]
    Destinations --> NewRelic[New Relic<br/>APM Platform]
    
    Prometheus --> Grafana[Grafana Dashboards]
    Jaeger --> TraceAnalysis[Trace Analysis]
    NewRelic --> AlertManager[Alert Manager]
    
    Grafana --> Alerts1[Quality Alerts]
    TraceAnalysis --> Alerts2[Performance Alerts]
    AlertManager --> Alerts3[System Alerts]
    
    Alerts1 --> End([Monitoring Complete])
    Alerts2 --> End
    Alerts3 --> End
```

## Configuration and Deployment Flow

```mermaid
flowchart TD
    Start([Service Startup]) --> LoadConfig[Load Configuration]
    LoadConfig --> ValidateConfig[Validate Configuration]
    
    ValidateConfig -->|Valid| InitComponents[Initialize Components]
    ValidateConfig -->|Invalid| ConfigError[Configuration Error]
    ConfigError --> Exit[Exit with Error]
    
    InitComponents --> InitKafka[Initialize Kafka Consumer]
    InitComponents --> InitSQS[Initialize SQS Consumer]
    InitComponents --> InitValidators[Initialize Validators]
    InitComponents --> InitStorage[Initialize Storage]
    InitComponents --> InitOTEL[Initialize OTEL]
    
    InitKafka --> HealthCheck1[Kafka Health Check]
    InitSQS --> HealthCheck2[SQS Health Check]
    InitValidators --> HealthCheck3[Validator Health Check]
    InitStorage --> HealthCheck4[Storage Health Check]
    InitOTEL --> HealthCheck5[OTEL Health Check]
    
    HealthCheck1 --> StartConsumers[Start Message Consumers]
    HealthCheck2 --> StartConsumers
    HealthCheck3 --> StartConsumers
    HealthCheck4 --> StartConsumers
    HealthCheck5 --> StartConsumers
    
    StartConsumers --> RegisterHandlers[Register Signal Handlers]
    RegisterHandlers --> ServiceReady[Service Ready]
    
    ServiceReady --> ProcessMessages[Process Messages]
    ProcessMessages --> GracefulShutdown{Shutdown Signal?}
    
    GracefulShutdown -->|No| ProcessMessages
    GracefulShutdown -->|Yes| StopConsumers[Stop Message Consumers]
    
    StopConsumers --> FlushMetrics[Flush Metrics]
    FlushMetrics --> CloseConnections[Close Connections]
    CloseConnections --> ServiceStopped[Service Stopped]
    
    Exit --> End([End])
    ServiceStopped --> End
```

These Mermaid diagrams provide comprehensive visualization of:

1. **Overall System Architecture** - High-level component relationships
2. **Message Processing Flow** - Detailed sequence of message handling
3. **Topic-Specific Flows** - Specialized validation for different message types
4. **Address Validation Flow** - Comprehensive address validation logic
5. **Data Quality Scoring** - Quality metric calculation and classification
6. **Error Handling** - Robust error recovery and circuit breaker patterns
7. **Observability Flow** - Complete monitoring and alerting pipeline
8. **Configuration/Deployment** - Service lifecycle and health management

Each diagram focuses on a specific aspect of the validation service, making it easy to understand the flow and implement the corresponding code components.