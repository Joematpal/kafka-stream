package main

import (
	"encoding/json"
	"testing"
)

func TestMergePatch_RemoveOperations(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		patch    string
		expected string
	}{
		{
			name:     "Remove string field with empty string",
			target:   `{"name": "John", "city": "NYC"}`,
			patch:    `{"city": ""}`, // empty string removes city
			expected: `{"name": "John"}`,
		},
		{
			name:     "Remove int field with zero",
			target:   `{"name": "John", "age": 30}`,
			patch:    `{"age": 0}`, // zero removes age
			expected: `{"name": "John"}`,
		},
		{
			name:     "Remove bool field with false",
			target:   `{"name": "John", "active": true}`,
			patch:    `{"active": false}`, // false removes active
			expected: `{"name": "John"}`,
		},
		{
			name:     "Remove field with null",
			target:   `{"name": "John", "city": "NYC"}`,
			patch:    `{"city": null}`, // null removes city
			expected: `{"name": "John"}`,
		},
		{
			name:     "Remove array field with empty array",
			target:   `{"name": "John", "tags": ["a", "b"]}`,
			patch:    `{"tags": []}`, // empty array removes tags
			expected: `{"name": "John"}`,
		},
		{
			name:     "Remove object field with empty object",
			target:   `{"name": "John", "address": {"street": "123 Main"}}`,
			patch:    `{"address": {}}`, // empty object removes address
			expected: `{"name": "John"}`,
		},
		{
			name:     "Multiple removals and updates",
			target:   `{"name": "John", "age": 30, "city": "NYC", "active": true}`,
			patch:    `{"age": 31, "city": "", "country": "USA"}`, // update age, remove city, add country
			expected: `{"name": "John", "age": 31, "active": true, "country": "USA"}`,
		},
		{
			name:     "Nested object removal",
			target:   `{"user": {"name": "John", "city": "NYC"}, "meta": {"version": 1}}`,
			patch:    `{"user": {"city": ""}}`, // remove nested city
			expected: `{"user": {"name": "John"}, "meta": {"version": 1}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergePatch([]byte(tt.target), []byte(tt.patch))
			if err != nil {
				t.Fatalf("MergePatch failed: %v", err)
			}

			// Compare JSON objects (order-independent)
			var expectedObj, resultObj interface{}
			if err := json.Unmarshal([]byte(tt.expected), &expectedObj); err != nil {
				t.Fatalf("Failed to unmarshal expected: %v", err)
			}
			if err := json.Unmarshal(result, &resultObj); err != nil {
				t.Fatalf("Failed to unmarshal result: %v", err)
			}

			expectedJSON, _ := json.Marshal(expectedObj)
			resultJSON, _ := json.Marshal(resultObj)

			if string(expectedJSON) != string(resultJSON) {
				t.Errorf("Expected: %s, Got: %s", expectedJSON, resultJSON)
			}
		})
	}
}

func TestMergeStruct_RemoveOperations(t *testing.T) {
	tests := []struct {
		name     string
		target   User
		patch    User
		expected User
	}{
		{
			name:     "Remove city with omitempty",
			target:   User{Name: "John", Age: 30, City: "NYC"},
			patch:    User{Age: 31}, // City omitted = removed due to omitempty
			expected: User{Name: "John", Age: 31},
		},
		{
			name:     "Remove multiple fields",
			target:   User{Name: "John", Age: 30, City: "NYC", Country: "USA"},
			patch:    User{Name: "Jane"}, // Only name provided, others removed
			expected: User{Name: "Jane"},
		},
		{
			name:     "Add and remove fields",
			target:   User{Name: "John", City: "NYC"},
			patch:    User{Name: "John", Age: 25, Country: "USA"}, // Remove city, add age and country
			expected: User{Name: "John", Age: 25, Country: "USA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeStruct(tt.target, tt.patch)
			if err != nil {
				t.Fatalf("MergeStruct failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected: %+v, Got: %+v", tt.expected, result)
			}
		})
	}
}

func TestMerge_Generic(t *testing.T) {
	tests := []struct {
		name     string
		target   User
		patch    User
		expected User
	}{
		{
			name:     "Generic merge with field removal",
			target:   User{Name: "Alice", Age: 28, City: "Boston"},
			patch:    User{Name: "Alice", Country: "Canada"}, // Remove age and city, add country
			expected: User{Name: "Alice", Country: "Canada"},
		},
		{
			name:     "Generic merge with updates",
			target:   User{Name: "Bob", Age: 35},
			patch:    User{Name: "Robert", Age: 36, City: "Seattle"},
			expected: User{Name: "Robert", Age: 36, City: "Seattle"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Merge(tt.target, tt.patch)
			if err != nil {
				t.Fatalf("Merge failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected: %+v, Got: %+v", tt.expected, result)
			}
		})
	}
}

func TestMustMerge(t *testing.T) {
	target := User{Name: "Charlie", Age: 40, City: "Denver"}
	patch := User{Name: "Charles", Country: "Mexico"} // Remove age and city
	expected := User{Name: "Charles", Country: "Mexico"}

	result := MustMerge(target, patch)
	if result != expected {
		t.Errorf("Expected: %+v, Got: %+v", expected, result)
	}
}

func TestMergeInto(t *testing.T) {
	target := User{Name: "Diana", Age: 32, City: "Miami"}
	patch := User{Age: 33, Country: "Brazil"} // Update age, remove city, add country
	expected := User{Name: "Diana", Age: 33, Country: "Brazil"}

	err := MergeInto(&target, patch)
	if err != nil {
		t.Fatalf("MergeInto failed: %v", err)
	}

	if target != expected {
		t.Errorf("Expected: %+v, Got: %+v", expected, target)
	}
}

// Test with different struct types to show generics work
type Product struct {
	ID    int     `json:"id,omitempty"`
	Name  string  `json:"name,omitempty"`
	Price float64 `json:"price,omitempty"`
}

func TestMerge_DifferentTypes(t *testing.T) {
	product := Product{ID: 1, Name: "Widget", Price: 9.99}
	patch := Product{Name: "Super Widget", Price: 12.99} // Remove ID
	expected := Product{Name: "Super Widget", Price: 12.99}

	result, err := Merge(product, patch)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if result != expected {
		t.Errorf("Expected: %+v, Got: %+v", expected, result)
	}
}

func TestIsZeroValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"nil", nil, true},
		{"empty string", "", true},
		{"non-empty string", "hello", false},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"zero float", 0.0, true},
		{"non-zero float", 3.14, false},
		{"empty slice", []string{}, true},
		{"non-empty slice", []string{"a"}, false},
		{"empty map", map[string]string{}, true},
		{"non-empty map", map[string]string{"a": "b"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isZeroValue(tt.value)
			if result != tt.expected {
				t.Errorf("isZeroValue(%v) = %v, expected %v", tt.value, result, tt.expected)
			}
		})
	}
}

// Benchmark to show performance
func BenchmarkMergePatch(b *testing.B) {
	target := []byte(`{"name": "John", "age": 30, "city": "NYC", "active": true}`)
	patch := []byte(`{"age": 31, "city": "", "country": "USA"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergePatch(target, patch)
		if err != nil {
			b.Fatal(err)
		}
	}
}
