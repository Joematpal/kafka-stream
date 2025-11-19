package validator

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// ISO20022Address represents an address in ISO20022 format
type ISO20022Address struct {
	AddressType    string `json:"address_type"` // ADDR, PBOX, HOME, BIZZ
	Department     string `json:"department"`
	SubDepartment  string `json:"sub_department"`
	StreetName     string `json:"street_name"`
	BuildingNumber string `json:"building_number"`
	PostCode       string `json:"post_code"`
	TownName       string `json:"town_name"`
	CountrySubDiv  string `json:"country_sub_div"`
	Country        string `json:"country"` // ISO 3166-1 alpha-2
}

// AddressValidationResult represents the result of address validation
type AddressValidationResult struct {
	IsValid           bool                `json:"is_valid"`
	Confidence        float64             `json:"confidence"` // 0.0 to 1.0
	Errors            []string            `json:"errors"`
	Warnings          []string            `json:"warnings"`
	Suggestions       []AddressSuggestion `json:"suggestions"`
	NormalizedAddress *ISO20022Address    `json:"normalized_address,omitempty"`
}

// AddressSuggestion represents a suggested address correction
type AddressSuggestion struct {
	Field      string  `json:"field"`
	Original   string  `json:"original"`
	Suggested  string  `json:"suggested"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason"`
}

// Enhanced FactorAddressValidator interface
type EnhancedAddressValidator interface {
	FactorAddressValidator
	ValidateISO20022Address(ctx context.Context, addr ISO20022Address) (*AddressValidationResult, error)
	SuggestCorrections(ctx context.Context, addr any) ([]AddressSuggestion, error)
	NormalizeAddress(ctx context.Context, addr any) (*ISO20022Address, error)
}

// DefaultAddressValidator provides basic address validation
type DefaultAddressValidator struct {
	countryRegex map[string]*regexp.Regexp
	postalRegex  map[string]*regexp.Regexp
}

// NewDefaultAddressValidator creates a new default address validator
func NewDefaultAddressValidator() *DefaultAddressValidator {
	return &DefaultAddressValidator{
		countryRegex: map[string]*regexp.Regexp{
			"US": regexp.MustCompile(`^[A-Z]{2}$`),
			"CA": regexp.MustCompile(`^[A-Z]{2}$`),
			"GB": regexp.MustCompile(`^[A-Z]{2}$`),
		},
		postalRegex: map[string]*regexp.Regexp{
			"US": regexp.MustCompile(`^\d{5}(-\d{4})?$`),
			"CA": regexp.MustCompile(`^[A-Z]\d[A-Z] \d[A-Z]\d$`),
			"GB": regexp.MustCompile(`^[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}$`),
		},
	}
}

// ValidateAddress implements the basic FactorAddressValidator interface
func (v *DefaultAddressValidator) ValidateAddress(addr any) error {
	// Convert to ISO20022Address if possible
	iso20022Addr, err := v.convertToISO20022(addr)
	if err != nil {
		return fmt.Errorf("failed to convert address to ISO20022 format: %w", err)
	}

	result, err := v.ValidateISO20022Address(context.Background(), *iso20022Addr)
	if err != nil {
		return err
	}

	if !result.IsValid {
		return fmt.Errorf("address validation failed: %s", strings.Join(result.Errors, "; "))
	}

	return nil
}

// ValidateISO20022Address validates an ISO20022 formatted address
func (v *DefaultAddressValidator) ValidateISO20022Address(ctx context.Context, addr ISO20022Address) (*AddressValidationResult, error) {
	result := &AddressValidationResult{
		IsValid:     true,
		Confidence:  1.0,
		Errors:      make([]string, 0),
		Warnings:    make([]string, 0),
		Suggestions: make([]AddressSuggestion, 0),
	}

	// Validate required fields
	if addr.Country == "" {
		result.IsValid = false
		result.Errors = append(result.Errors, "country is required")
	}

	if addr.TownName == "" {
		result.IsValid = false
		result.Errors = append(result.Errors, "town name is required")
	}

	// Validate country code format
	if addr.Country != "" {
		if len(addr.Country) != 2 {
			result.IsValid = false
			result.Errors = append(result.Errors, "country must be ISO 3166-1 alpha-2 code (2 characters)")
		} else {
			addr.Country = strings.ToUpper(addr.Country)
		}
	}

	// Validate postal code format based on country
	if addr.PostCode != "" && addr.Country != "" {
		if regex, exists := v.postalRegex[addr.Country]; exists {
			if !regex.MatchString(addr.PostCode) {
				result.IsValid = false
				result.Errors = append(result.Errors, fmt.Sprintf("invalid postal code format for country %s", addr.Country))
				result.Suggestions = append(result.Suggestions, AddressSuggestion{
					Field:      "post_code",
					Original:   addr.PostCode,
					Suggested:  v.suggestPostalCode(addr.PostCode, addr.Country),
					Confidence: 0.7,
					Reason:     "postal code format correction",
				})
			}
		}
	}

	// Validate address type
	if addr.AddressType != "" {
		validTypes := []string{"ADDR", "PBOX", "HOME", "BIZZ"}
		isValidType := false
		for _, validType := range validTypes {
			if addr.AddressType == validType {
				isValidType = true
				break
			}
		}
		if !isValidType {
			result.IsValid = false
			result.Errors = append(result.Errors, fmt.Sprintf("invalid address type: %s. Valid types: %s", addr.AddressType, strings.Join(validTypes, ", ")))
		}
	}

	// Check for completeness warnings
	if addr.StreetName == "" && addr.AddressType != "PBOX" {
		result.Warnings = append(result.Warnings, "street name is missing")
		result.Confidence *= 0.8
	}

	if addr.PostCode == "" {
		result.Warnings = append(result.Warnings, "postal code is missing")
		result.Confidence *= 0.9
	}

	// Normalize the address
	normalizedAddr := v.normalizeISO20022Address(addr)
	result.NormalizedAddress = &normalizedAddr

	return result, nil
}

// SuggestCorrections suggests corrections for address fields
func (v *DefaultAddressValidator) SuggestCorrections(ctx context.Context, addr any) ([]AddressSuggestion, error) {
	iso20022Addr, err := v.convertToISO20022(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert address to ISO20022 format: %w", err)
	}

	result, err := v.ValidateISO20022Address(ctx, *iso20022Addr)
	if err != nil {
		return nil, err
	}

	return result.Suggestions, nil
}

// NormalizeAddress normalizes an address to ISO20022 format
func (v *DefaultAddressValidator) NormalizeAddress(ctx context.Context, addr any) (*ISO20022Address, error) {
	iso20022Addr, err := v.convertToISO20022(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert address to ISO20022 format: %w", err)
	}

	normalized := v.normalizeISO20022Address(*iso20022Addr)
	return &normalized, nil
}

// convertToISO20022 converts various address formats to ISO20022Address
func (v *DefaultAddressValidator) convertToISO20022(addr any) (*ISO20022Address, error) {
	switch a := addr.(type) {
	case ISO20022Address:
		return &a, nil
	case *ISO20022Address:
		return a, nil
	case map[string]interface{}:
		return v.mapToISO20022(a), nil
	default:
		return nil, fmt.Errorf("unsupported address type: %T", addr)
	}
}

// mapToISO20022 converts a map to ISO20022Address
func (v *DefaultAddressValidator) mapToISO20022(m map[string]interface{}) *ISO20022Address {
	addr := &ISO20022Address{}

	if val, ok := m["address_type"].(string); ok {
		addr.AddressType = val
	}
	if val, ok := m["department"].(string); ok {
		addr.Department = val
	}
	if val, ok := m["sub_department"].(string); ok {
		addr.SubDepartment = val
	}
	if val, ok := m["street_name"].(string); ok {
		addr.StreetName = val
	}
	if val, ok := m["building_number"].(string); ok {
		addr.BuildingNumber = val
	}
	if val, ok := m["post_code"].(string); ok {
		addr.PostCode = val
	}
	if val, ok := m["town_name"].(string); ok {
		addr.TownName = val
	}
	if val, ok := m["country_sub_div"].(string); ok {
		addr.CountrySubDiv = val
	}
	if val, ok := m["country"].(string); ok {
		addr.Country = val
	}

	return addr
}

// normalizeISO20022Address normalizes an ISO20022Address
func (v *DefaultAddressValidator) normalizeISO20022Address(addr ISO20022Address) ISO20022Address {
	normalized := addr

	// Normalize country code to uppercase
	normalized.Country = strings.ToUpper(strings.TrimSpace(addr.Country))

	// Normalize address type to uppercase
	normalized.AddressType = strings.ToUpper(strings.TrimSpace(addr.AddressType))

	// Trim whitespace from all fields
	normalized.Department = strings.TrimSpace(addr.Department)
	normalized.SubDepartment = strings.TrimSpace(addr.SubDepartment)
	normalized.StreetName = strings.TrimSpace(addr.StreetName)
	normalized.BuildingNumber = strings.TrimSpace(addr.BuildingNumber)
	normalized.PostCode = strings.TrimSpace(addr.PostCode)
	normalized.TownName = strings.TrimSpace(addr.TownName)
	normalized.CountrySubDiv = strings.TrimSpace(addr.CountrySubDiv)

	// Normalize postal code format based on country
	if normalized.Country == "US" && normalized.PostCode != "" {
		// Remove any non-digit characters and format as XXXXX or XXXXX-XXXX
		digits := regexp.MustCompile(`\D`).ReplaceAllString(normalized.PostCode, "")
		if len(digits) == 5 {
			normalized.PostCode = digits
		} else if len(digits) == 9 {
			normalized.PostCode = digits[:5] + "-" + digits[5:]
		}
	}

	return normalized
}

// suggestPostalCode suggests a corrected postal code format
func (v *DefaultAddressValidator) suggestPostalCode(original, country string) string {
	switch country {
	case "US":
		// Remove non-digits and format
		digits := regexp.MustCompile(`\D`).ReplaceAllString(original, "")
		if len(digits) >= 5 {
			if len(digits) >= 9 {
				return digits[:5] + "-" + digits[5:9]
			}
			return digits[:5]
		}
	case "CA":
		// Format as A1A 1A1
		alphanumeric := regexp.MustCompile(`[^A-Z0-9]`).ReplaceAllString(strings.ToUpper(original), "")
		if len(alphanumeric) >= 6 {
			return alphanumeric[:3] + " " + alphanumeric[3:6]
		}
	}
	return original
}
