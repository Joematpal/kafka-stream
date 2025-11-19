package validator

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// ValidationRule represents a single validation rule
type ValidationRule struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Severity    ValidationSeverity `json:"severity"`
	Field       string             `json:"field"`    // JSON path to field
	Operator    string             `json:"operator"` // eq, ne, gt, lt, regex, required, etc.
	Value       interface{}        `json:"value"`    // expected value
	Enabled     bool               `json:"enabled"`
}

// RuleEngine manages and executes validation rules
type RuleEngine struct {
	rules map[string][]ValidationRule // topic -> rules
}

func NewRuleEngine() *RuleEngine {
	return &RuleEngine{
		rules: make(map[string][]ValidationRule),
	}
}

// AddRule adds a validation rule for a specific topic
func (re *RuleEngine) AddRule(topic string, rule ValidationRule) {
	if re.rules[topic] == nil {
		re.rules[topic] = make([]ValidationRule, 0)
	}
	re.rules[topic] = append(re.rules[topic], rule)
}

// GetRules returns all rules for a topic
func (re *RuleEngine) GetRules(topic string) []ValidationRule {
	return re.rules[topic]
}

// ValidateMessage validates a message against all rules for its topic
func (re *RuleEngine) ValidateMessage(ctx context.Context, topic string, data interface{}) []ValidationResult {
	rules := re.GetRules(topic)
	results := make([]ValidationResult, 0)

	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}

		result := re.executeRule(rule, data)
		results = append(results, result)
	}

	return results
}

// ValidationResult represents the result of applying a validation rule
type ValidationResult struct {
	RuleID        string             `json:"rule_id"`
	RuleName      string             `json:"rule_name"`
	Field         string             `json:"field"`
	Severity      ValidationSeverity `json:"severity"`
	IsValid       bool               `json:"is_valid"`
	ErrorMessage  string             `json:"error_message,omitempty"`
	ActualValue   any                `json:"actual_value,omitempty"`
	ExpectedValue any                `json:"expected_value,omitempty"`
}

// executeRule executes a single validation rule against data
func (re *RuleEngine) executeRule(rule ValidationRule, data interface{}) ValidationResult {
	result := ValidationResult{
		RuleID:        rule.ID,
		RuleName:      rule.Name,
		Field:         rule.Field,
		Severity:      rule.Severity,
		ExpectedValue: rule.Value,
	}

	// Extract field value from data
	fieldValue, err := extractFieldValue(data, rule.Field)
	if err != nil {
		result.IsValid = false
		result.ErrorMessage = fmt.Sprintf("Failed to extract field '%s': %v", rule.Field, err)
		return result
	}

	result.ActualValue = fieldValue

	// Apply validation operator
	switch rule.Operator {
	case "required":
		result.IsValid = fieldValue != nil && !isEmptyValue(fieldValue)
		if !result.IsValid {
			result.ErrorMessage = fmt.Sprintf("Field '%s' is required but missing or empty", rule.Field)
		}

	case "eq":
		result.IsValid = compareValues(fieldValue, rule.Value, "eq")
		if !result.IsValid {
			result.ErrorMessage = fmt.Sprintf("Field '%s' expected '%v' but got '%v'", rule.Field, rule.Value, fieldValue)
		}

	case "ne":
		result.IsValid = compareValues(fieldValue, rule.Value, "ne")
		if !result.IsValid {
			result.ErrorMessage = fmt.Sprintf("Field '%s' should not equal '%v'", rule.Field, rule.Value)
		}

	case "regex":
		result.IsValid = matchesRegex(fieldValue, rule.Value)
		if !result.IsValid {
			result.ErrorMessage = fmt.Sprintf("Field '%s' does not match pattern '%v'", rule.Field, rule.Value)
		}

	case "type":
		result.IsValid = checkType(fieldValue, rule.Value)
		if !result.IsValid {
			result.ErrorMessage = fmt.Sprintf("Field '%s' expected type '%v' but got '%T'", rule.Field, rule.Value, fieldValue)
		}

	default:
		result.IsValid = false
		result.ErrorMessage = fmt.Sprintf("Unknown validation operator: %s", rule.Operator)
	}

	return result
}

// extractFieldValue extracts a field value from data using JSON path notation
func extractFieldValue(data interface{}, fieldPath string) (interface{}, error) {
	if fieldPath == "" || fieldPath == "." {
		return data, nil
	}

	// Convert data to map for easier navigation
	var dataMap map[string]interface{}

	// Handle different data types
	switch v := data.(type) {
	case map[string]interface{}:
		dataMap = v
	case []byte:
		if err := json.Unmarshal(v, &dataMap); err != nil {
			return nil, err
		}
	case string:
		if err := json.Unmarshal([]byte(v), &dataMap); err != nil {
			return nil, err
		}
	default:
		// Try to marshal and unmarshal to get a map
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(jsonData, &dataMap); err != nil {
			return nil, err
		}
	}

	// Navigate the field path
	parts := strings.Split(fieldPath, ".")
	current := interface{}(dataMap)

	for _, part := range parts {
		if part == "" {
			continue
		}

		switch curr := current.(type) {
		case map[string]interface{}:
			current = curr[part]
		default:
			return nil, fmt.Errorf("cannot navigate to field '%s' in path '%s'", part, fieldPath)
		}

		if current == nil {
			return nil, nil // Field not found
		}
	}

	return current, nil
}

// isEmptyValue checks if a value is considered empty
func isEmptyValue(value interface{}) bool {
	if value == nil {
		return true
	}

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return v.Len() == 0
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	}

	return false
}

// compareValues compares two values based on operator
func compareValues(actual, expected interface{}, operator string) bool {
	if actual == nil && expected == nil {
		return operator == "eq"
	}
	if actual == nil || expected == nil {
		return operator == "ne"
	}

	switch operator {
	case "eq":
		return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
	case "ne":
		return fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected)
	}

	return false
}

// matchesRegex checks if a value matches a regex pattern
func matchesRegex(value, pattern interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	patternStr := fmt.Sprintf("%v", pattern)

	matched, err := regexp.MatchString(patternStr, valueStr)
	return err == nil && matched
}

// checkType validates the type of a value
func checkType(value, expectedType interface{}) bool {
	if value == nil {
		return false
	}

	expectedTypeStr := fmt.Sprintf("%v", expectedType)

	switch expectedTypeStr {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case int, int32, int64, float32, float64:
			return true
		case string:
			_, err := strconv.ParseFloat(value.(string), 64)
			return err == nil
		}
		return false
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		v := reflect.ValueOf(value)
		return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	}

	return false
}
