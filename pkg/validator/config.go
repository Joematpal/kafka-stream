package validator

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// ValidationServiceConfig represents the complete configuration for the validation service
type ValidationServiceConfig struct {
	Service       ServiceConfig       `json:"service" yaml:"service"`
	Sources       SourcesConfig       `json:"sources" yaml:"sources"`
	Validation    ValidationConfig    `json:"validation" yaml:"validation"`
	Storage       StorageConfig       `json:"storage" yaml:"storage"`
	Observability ObservabilityConfig `json:"observability" yaml:"observability"`
}

// ServiceConfig contains basic service configuration
type ServiceConfig struct {
	InstanceID string `json:"instance_id" yaml:"instance_id"`
	Name       string `json:"name" yaml:"name"`
	Version    string `json:"version" yaml:"version"`
	LogLevel   string `json:"log_level" yaml:"log_level"`
}

// SourcesConfig configures message sources
type SourcesConfig struct {
	Kafka KafkaConfig `json:"kafka" yaml:"kafka"`
	SQS   SQSConfig   `json:"sqs" yaml:"sqs"`
}

// TopicConfig represents configuration for a specific topic/queue
type TopicConfig struct {
	Name     string   `json:"name" yaml:"name"`         // topic/queue name
	Handlers []string `json:"handlers" yaml:"handlers"` // validation handlers to apply
}

// KafkaConfig configures Kafka message source
type KafkaConfig struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	Brokers       []string      `json:"brokers" yaml:"brokers"`
	ConsumerGroup string        `json:"consumer_group" yaml:"consumer_group"`
	Topics        []TopicConfig `json:"topics" yaml:"topics"`
	BatchSize     int           `json:"batch_size" yaml:"batch_size"`
	Timeout       string        `json:"timeout" yaml:"timeout"`
}

// SQSConfig configures SQS message source
type SQSConfig struct {
	Enabled   bool          `json:"enabled" yaml:"enabled"`
	Region    string        `json:"region" yaml:"region"`
	Queues    []QueueConfig `json:"queues" yaml:"queues"`
	BatchSize int           `json:"batch_size" yaml:"batch_size"`
	Timeout   string        `json:"timeout" yaml:"timeout"`
}

// QueueConfig represents SQS queue configuration
type QueueConfig struct {
	Name     string   `json:"name" yaml:"name"`
	URL      string   `json:"url" yaml:"url"`
	Handlers []string `json:"handlers" yaml:"handlers"`
}

// ValidationConfig configures validation behavior
type ValidationConfig struct {
	RulesConfigPath    string                 `json:"rules_config_path" yaml:"rules_config_path"`
	AddressValidator   AddressValidatorConfig `json:"address_validator" yaml:"address_validator"`
	ParallelProcessing bool                   `json:"parallel_processing" yaml:"parallel_processing"`
	MaxWorkers         int                    `json:"max_workers" yaml:"max_workers"`
	BatchSize          int                    `json:"batch_size" yaml:"batch_size"`
	Timeout            string                 `json:"timeout" yaml:"timeout"`
}

// AddressValidatorConfig configures address validation
type AddressValidatorConfig struct {
	Provider        string `json:"provider" yaml:"provider"`
	ISO20022Enabled bool   `json:"iso20022_enabled" yaml:"iso20022_enabled"`
	APIKey          string `json:"api_key" yaml:"api_key"`
	Timeout         string `json:"timeout" yaml:"timeout"`
}

// StorageConfig configures data storage
type StorageConfig struct {
	DataQualityStore DataQualityStoreConfig `json:"data_quality_store" yaml:"data_quality_store"`
}

// DataQualityStoreConfig configures the data quality store
type DataQualityStoreConfig struct {
	Type             string `json:"type" yaml:"type"`
	ConnectionString string `json:"connection_string" yaml:"connection_string"`
	TableName        string `json:"table_name" yaml:"table_name"`
	BatchSize        int    `json:"batch_size" yaml:"batch_size"`
	FlushInterval    string `json:"flush_interval" yaml:"flush_interval"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *ValidationServiceConfig {
	return &ValidationServiceConfig{
		Service: ServiceConfig{
			InstanceID: "validator-001",
			Name:       "validation-service",
			Version:    "1.0.0",
			LogLevel:   "info",
		},
		Sources: SourcesConfig{
			Kafka: KafkaConfig{
				Enabled:       true,
				Brokers:       []string{"localhost:9092"},
				ConsumerGroup: "validation-service",
				Topics: []TopicConfig{
					{
						Name:     "topic_a",
						Handlers: []string{"generic_validation"},
					},
					{
						Name:     "ebe_v11",
						Handlers: []string{"ebe_validation", "address_validation"},
					},
				},
				BatchSize: 100,
				Timeout:   "30s",
			},
			SQS: SQSConfig{
				Enabled:   false,
				Region:    "us-east-1",
				Queues:    []QueueConfig{},
				BatchSize: 10,
				Timeout:   "30s",
			},
		},
		Validation: ValidationConfig{
			RulesConfigPath: "./config/validation_rules.json",
			AddressValidator: AddressValidatorConfig{
				Provider:        "default",
				ISO20022Enabled: true,
				Timeout:         "10s",
			},
			ParallelProcessing: true,
			MaxWorkers:         10,
			BatchSize:          100,
			Timeout:            "60s",
		},
		Storage: StorageConfig{
			DataQualityStore: DataQualityStoreConfig{
				Type:             "postgresql",
				ConnectionString: "postgres://user:pass@localhost/db",
				TableName:        "data_quality_results",
				BatchSize:        1000,
				FlushInterval:    "30s",
			},
		},
		Observability: ObservabilityConfig{
			ServiceName:    "validation-service",
			ServiceVersion: "1.0.0",
			Enabled:        true,
			MetricsEnabled: true,
			TracingEnabled: true,
			SampleRate:     0.1,
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(configPath string) (*ValidationServiceConfig, error) {
	// Start with default config
	config := DefaultConfig()

	// If no config file specified, return default
	if configPath == "" {
		return config, nil
	}

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return config, fmt.Errorf("config file not found: %s", configPath)
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse JSON config (could be extended to support YAML)
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// Validate validates the configuration
func (c *ValidationServiceConfig) Validate() error {
	// Validate service config
	if c.Service.InstanceID == "" {
		return fmt.Errorf("service.instance_id is required")
	}
	if c.Service.Name == "" {
		return fmt.Errorf("service.name is required")
	}

	// Validate that at least one source is enabled
	if !c.Sources.Kafka.Enabled && !c.Sources.SQS.Enabled {
		return fmt.Errorf("at least one message source must be enabled")
	}

	// Validate Kafka config if enabled
	if c.Sources.Kafka.Enabled {
		if len(c.Sources.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka.brokers is required when kafka is enabled")
		}
		if c.Sources.Kafka.ConsumerGroup == "" {
			return fmt.Errorf("kafka.consumer_group is required when kafka is enabled")
		}
		if len(c.Sources.Kafka.Topics) == 0 {
			return fmt.Errorf("kafka.topics is required when kafka is enabled")
		}
	}

	// Validate SQS config if enabled
	if c.Sources.SQS.Enabled {
		if c.Sources.SQS.Region == "" {
			return fmt.Errorf("sqs.region is required when sqs is enabled")
		}
		if len(c.Sources.SQS.Queues) == 0 {
			return fmt.Errorf("sqs.queues is required when sqs is enabled")
		}
	}

	// Validate validation config
	if c.Validation.MaxWorkers <= 0 {
		return fmt.Errorf("validation.max_workers must be greater than 0")
	}
	if c.Validation.BatchSize <= 0 {
		return fmt.Errorf("validation.batch_size must be greater than 0")
	}

	// Validate storage config
	if c.Storage.DataQualityStore.Type == "" {
		return fmt.Errorf("storage.data_quality_store.type is required")
	}
	if c.Storage.DataQualityStore.ConnectionString == "" {
		return fmt.Errorf("storage.data_quality_store.connection_string is required")
	}

	return nil
}

// GetTimeout parses a timeout string and returns a duration
func (c *ValidationServiceConfig) GetTimeout(timeoutStr string) (time.Duration, error) {
	if timeoutStr == "" {
		return 30 * time.Second, nil
	}
	return time.ParseDuration(timeoutStr)
}

// SaveConfig saves the configuration to a file
func (c *ValidationServiceConfig) SaveConfig(configPath string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ConfigManager manages configuration loading and reloading
type ConfigManager struct {
	config     *ValidationServiceConfig
	configPath string
	callbacks  []func(*ValidationServiceConfig)
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(configPath string) (*ConfigManager, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	return &ConfigManager{
		config:     config,
		configPath: configPath,
		callbacks:  make([]func(*ValidationServiceConfig), 0),
	}, nil
}

// GetConfig returns the current configuration
func (cm *ConfigManager) GetConfig() *ValidationServiceConfig {
	return cm.config
}

// ReloadConfig reloads the configuration from file
func (cm *ConfigManager) ReloadConfig() error {
	newConfig, err := LoadConfig(cm.configPath)
	if err != nil {
		return err
	}

	cm.config = newConfig

	// Notify callbacks of config change
	for _, callback := range cm.callbacks {
		callback(cm.config)
	}

	return nil
}

// OnConfigChange registers a callback for configuration changes
func (cm *ConfigManager) OnConfigChange(callback func(*ValidationServiceConfig)) {
	cm.callbacks = append(cm.callbacks, callback)
}
